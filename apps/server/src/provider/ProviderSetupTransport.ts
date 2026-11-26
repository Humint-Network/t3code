import {
  ProviderNativeEngineCheck,
  type ProviderNativeEngineStatus,
  ProviderNativeEngineAttestation,
  type ProviderNativeEngine,
  ProviderSetupTransportBegin,
  ProviderSetupTransportLease,
  ProviderSetupTransportComplete,
  ProviderSetupTransportUnavailable,
  type ProviderSetupTransportRequest,
  type ProviderSetupTransportResponse,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Hex from "effect/encoding/Hex";
import { HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/http";

type Engine = ProviderNativeEngine & {
  readonly check: string;
  readonly phase: ProviderNativeEngineStatus["phase"];
};
type Error = ProviderSetupTransportUnavailable;
type Job = {
  request: ProviderSetupTransportRequest;
  taken: boolean;
  resume: (effect: Effect.Effect<ProviderSetupTransportResponse, Error>) => void;
};
type Lease = {
  id: string;
  instanceId: ProviderInstanceId;
  generation: object;
  expires: number;
  jobs: Map<string, Job>;
};
const unavailable = () => new ProviderSetupTransportUnavailable();
const utf8 = (text: string) => new TextEncoder().encode(text).byteLength;
const decodeCheck = Schema.decodeUnknownEffect(ProviderNativeEngineCheck);
const decodeAttestation = Schema.decodeUnknownEffect(ProviderNativeEngineAttestation);
const decodeBegin = Schema.decodeUnknownEffect(ProviderSetupTransportBegin);
const decodeLease = Schema.decodeUnknownEffect(ProviderSetupTransportLease);
const decodeComplete = Schema.decodeUnknownEffect(ProviderSetupTransportComplete);
const strict = { onExcessProperty: "error" } as const;

// This is an OAuth/catalog adapter, never a generic HTTP proxy. The native side
// independently enforces the same destinations before using any private payload.
function project(request: HttpClientRequest.HttpClientRequest) {
  const url = new URL(request.url);
  if (
    request.url.length > 2048 ||
    url.href !== request.url ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    request.urlParams.params.length ||
    request.hash._tag !== "None"
  )
    throw unavailable();
  const catalog = url.href === "https://api.openai.com/v1/models";
  if (!catalog && url.origin !== "https://auth.openai.com") throw unavailable();
  if (request.method !== "GET" && request.method !== "POST") throw unavailable();
  const headers: Record<string, string> = {};
  for (const [key, value] of Object.entries(request.headers)) {
    if (key === "content-length") continue;
    if (
      !["accept", "content-type", "authorization"].includes(key) ||
      !/^[\x20-\x7e]{1,32775}$/.test(value)
    )
      throw unavailable();
    headers[key] = value;
  }
  if (headers.accept !== undefined && headers.accept !== "application/json") throw unavailable();
  let body = "";
  if (request.body._tag === "Uint8Array")
    body = new TextDecoder("utf-8", { fatal: true }).decode(request.body.body);
  else if (request.body._tag !== "Empty") throw unavailable();
  if (
    request.headers["content-length"] !== undefined &&
    request.headers["content-length"] !== String(utf8(body))
  )
    throw unavailable();
  if (utf8(body) > 65536) throw unavailable();
  if (request.method === "GET" && (body !== "" || headers["content-type"] !== undefined))
    throw unavailable();
  if (
    request.method === "POST" &&
    (catalog || !body || headers["content-type"] !== "application/x-www-form-urlencoded")
  )
    throw unavailable();
  if (
    catalog
      ? !/^Bearer [\x21-\x7e]{1,32768}$/.test(headers.authorization ?? "")
      : headers.authorization !== undefined
  )
    throw unavailable();
  return { method: request.method, url: request.url, headers, body } as const;
}

export class ProviderSetupTransport extends Context.Service<
  ProviderSetupTransport,
  {
    readonly attest: (
      input: ProviderNativeEngineAttestation,
    ) => Effect.Effect<ProviderNativeEngineStatus, Error>;
    readonly status: (
      input: ProviderNativeEngineCheck,
    ) => Effect.Effect<ProviderNativeEngineStatus, Error>;
    readonly settle: (
      instanceId: ProviderInstanceId,
      check: string,
      ready: boolean,
    ) => Effect.Effect<boolean>;
    readonly engine: (instanceId: ProviderInstanceId) => Effect.Effect<Engine, Error>;
    readonly engineChanges: (instanceId: ProviderInstanceId) => Stream.Stream<void, Error>;
    readonly client: (
      instanceId: ProviderInstanceId,
    ) => Effect.Effect<HttpClient.HttpClient, Error, Scope.Scope>;
    readonly begin: (
      input: ProviderSetupTransportBegin,
    ) => Effect.Effect<ProviderSetupTransportLease, Error>;
    readonly take: (
      input: ProviderSetupTransportLease,
    ) => Effect.Effect<ProviderSetupTransportRequest | null, Error>;
    readonly complete: (input: ProviderSetupTransportComplete) => Effect.Effect<void, Error>;
    readonly close: (input: ProviderSetupTransportLease) => Effect.Effect<void, Error>;
  }
>()("t3/provider/ProviderSetupTransport") {}

export const make = Effect.gen(function* () {
  const managed = yield* Config.Boolean("T3CODE_ICORE_PROJECTS").pipe(
    Config.withDefault(false),
    Effect.orDie,
  );
  const random = yield* SecureRandom.SecureRandom;
  const generations = new Map<ProviderInstanceId, object>();
  const leases = new Map<string, Lease>();
  const engines = new Map<object, Engine>();
  const signals = new Map<object, PubSub.PubSub<void>>();
  const retire = (lease: Lease) => {
    leases.delete(lease.id);
    for (const job of lease.jobs.values()) job.resume(Effect.fail(unavailable()));
    lease.jobs.clear();
  };
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      for (const lease of leases.values()) retire(lease);
      generations.clear();
    }),
  );
  const live = (lease: Lease, now: number) =>
    managed &&
    leases.get(lease.id) === lease &&
    generations.get(lease.instanceId) === lease.generation &&
    now < lease.expires;
  const sweep = (now: number) => {
    for (const lease of leases.values()) if (!live(lease, now)) retire(lease);
  };
  const lookup = Effect.fn(function* (raw: ProviderSetupTransportLease) {
    const input = yield* decodeLease(raw, strict);
    const now = yield* Clock.currentTimeMillis;
    sweep(now);
    const lease = leases.get(input.lease);
    if (!managed || !lease || !live(lease, now)) return yield* unavailable();
    return lease;
  });
  const safe = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(Effect.mapError(unavailable));
  const engine = Effect.fn("ProviderSetupTransport.engine")(function* (
    instanceId: ProviderInstanceId,
  ) {
    const generation = generations.get(instanceId);
    const proof = generation === undefined ? undefined : engines.get(generation);
    if (!managed || !proof) return yield* unavailable();
    return { ...proof };
  });
  const engineChanges = (instanceId: ProviderInstanceId) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const generation = generations.get(instanceId);
        const signal = generation === undefined ? undefined : signals.get(generation);
        if (!managed || !signal) return yield* unavailable();
        return Stream.fromPubSub(signal);
      }),
    );
  const attest = Effect.fn("ProviderSetupTransport.attest")(function* (
    raw: ProviderNativeEngineAttestation,
  ) {
    const input = yield* decodeAttestation(raw, strict);
    const lease = yield* lookup({ lease: input.lease });
    const previous = engines.get(lease.generation);
    if (
      previous &&
      (previous.revision !== input.engine.revision || previous.version !== input.engine.version)
    )
      return yield* unavailable();
    const signal = signals.get(lease.generation);
    if (!signal) return yield* unavailable();
    const check = Hex.encode(yield* random.randomBytes(32));
    if (!live(lease, yield* Clock.currentTimeMillis)) return yield* unavailable();
    engines.set(lease.generation, { ...input.engine, check, phase: "pending" });
    yield* PubSub.publish(signal, undefined);
    return { check, phase: "pending" as const };
  }, safe);
  const status = Effect.fn("ProviderSetupTransport.status")(function* (
    raw: ProviderNativeEngineCheck,
  ) {
    const input = yield* decodeCheck(raw, strict);
    const lease = yield* lookup({ lease: input.lease });
    const proof = engines.get(lease.generation);
    if (!proof || proof.check !== input.check) return yield* unavailable();
    return { check: proof.check, phase: proof.phase };
  }, safe);
  const settle = (instanceId: ProviderInstanceId, check: string, ready: boolean) =>
    Effect.sync(() => {
      const generation = generations.get(instanceId);
      const proof = generation === undefined ? undefined : engines.get(generation);
      if (!managed || !generation || !proof || proof.check !== check || proof.phase !== "pending")
        return false;
      engines.set(generation, { ...proof, phase: ready ? "ready" : "unavailable" });
      return true;
    });
  const begin = Effect.fn("ProviderSetupTransport.begin")(function* (
    raw: ProviderSetupTransportBegin,
  ) {
    const input = yield* decodeBegin(raw, strict);
    const id = Hex.encode(yield* random.randomBytes(32));
    const now = yield* Clock.currentTimeMillis;
    sweep(now);
    const generation = generations.get(input.instanceId);
    if (
      !managed ||
      !generation ||
      leases.size >= 4 ||
      leases.has(id) ||
      [...leases.values()].some((lease) => lease.instanceId === input.instanceId)
    )
      return yield* unavailable();
    leases.set(id, {
      id,
      instanceId: input.instanceId,
      generation,
      expires: now + 600_000,
      jobs: new Map(),
    });
    return { lease: id };
  }, safe);
  const take = Effect.fn("ProviderSetupTransport.take")(function* (
    input: ProviderSetupTransportLease,
  ) {
    const lease = yield* lookup(input);
    for (const job of lease.jobs.values())
      if (!job.taken) {
        job.taken = true;
        return job.request;
      }
    return null;
  }, safe);
  const complete = Effect.fn("ProviderSetupTransport.complete")(function* (
    raw: ProviderSetupTransportComplete,
  ) {
    const input = yield* decodeComplete(raw, strict);
    const lease = yield* lookup({ lease: input.lease });
    const job = lease.jobs.get(input.id);
    if (
      !job ||
      !job.taken ||
      (input.response &&
        (utf8(input.response.body) > 1048576 ||
          (input.response.status >= 300 && input.response.status < 400)))
    )
      return yield* unavailable();
    lease.jobs.delete(input.id);
    job.resume(
      input.response === null ? Effect.fail(unavailable()) : Effect.succeed(input.response),
    );
  }, safe);
  const close = Effect.fn("ProviderSetupTransport.close")(function* (
    raw: ProviderSetupTransportLease,
  ) {
    const input = yield* decodeLease(raw, strict);
    if (!managed) return yield* unavailable();
    const lease = leases.get(input.lease);
    if (lease) retire(lease);
  }, safe);
  const client = Effect.fn("ProviderSetupTransport.client")(function* (
    instanceId: ProviderInstanceId,
  ) {
    if (!managed) return yield* unavailable();
    const generation = {};
    const previous = generations.get(instanceId);
    if (previous) {
      engines.delete(previous);
      const oldSignal = signals.get(previous);
      if (oldSignal) yield* PubSub.shutdown(oldSignal);
      signals.delete(previous);
    }
    signals.set(generation, yield* PubSub.sliding<void>(1));
    for (const lease of leases.values()) if (lease.instanceId === instanceId) retire(lease);
    generations.set(instanceId, generation);
    const signal = signals.get(generation)!;
    yield* Effect.addFinalizer(() => PubSub.shutdown(signal));
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        if (generations.get(instanceId) !== generation) return;
        generations.delete(instanceId);
        engines.delete(generation);
        signals.delete(generation);
        for (const lease of leases.values()) if (lease.instanceId === instanceId) retire(lease);
      }),
    );
    return HttpClient.make((request) =>
      Effect.gen(function* () {
        const projected = yield* Effect.try({ try: () => project(request), catch: unavailable });
        const id = Hex.encode(yield* random.randomBytes(32));
        const now = yield* Clock.currentTimeMillis;
        sweep(now);
        const lease = [...leases.values()].find(
          (entry) => entry.instanceId === instanceId && entry.generation === generation,
        );
        if (!lease || !live(lease, now) || lease.jobs.size >= 4 || lease.jobs.has(id))
          return yield* unavailable();
        const response = yield* Effect.callback<ProviderSetupTransportResponse, Error>((resume) => {
          lease.jobs.set(id, {
            request: {
              ...projected,
              id,
              lease: lease.id,
              instanceId,
              expiresAt: Math.min(lease.expires, now + 15_000),
            },
            taken: false,
            resume,
          });
          return Effect.sync(() => {
            lease.jobs.delete(id);
          });
        }).pipe(Effect.timeout("15 seconds"));
        if (!live(lease, yield* Clock.currentTimeMillis)) return yield* unavailable();
        return HttpClientResponse.fromWeb(
          request,
          new Response([204, 205].includes(response.status) ? null : response.body, {
            status: response.status,
            headers: { "content-type": "application/json" },
          }),
        );
      }).pipe(
        Effect.mapError(
          () =>
            new HttpClientError.HttpClientError({
              reason: new HttpClientError.TransportError({
                request: HttpClientRequest.get("https://auth.openai.com/"),
                description: "Return to iCore to complete account setup.",
              }),
            }),
        ),
      ),
    ).pipe(
      HttpClient.transform((effect) =>
        effect.pipe(
          Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
          Effect.mapError(
            () =>
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.TransportError({
                  request: HttpClientRequest.get("https://auth.openai.com/"),
                  description: "Return to iCore to complete account setup.",
                }),
              }),
          ),
        ),
      ),
    );
  });
  return ProviderSetupTransport.of({
    client,
    begin,
    take,
    complete,
    close,
    attest,
    engine,
    engineChanges,
    status,
    settle,
  });
});
export const layer = Layer.effect(ProviderSetupTransport, make);
