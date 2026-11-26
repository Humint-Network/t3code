// @effect-diagnostics nodeBuiltinImport:off - Synthetic issuer signs test identities; no external request.
import * as NodeCrypto from "node:crypto";
import * as NodePlatformCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderInstanceId,
  ProviderDriverKind,
  type ServerProvider,
} from "@t3tools/contracts";
import * as ConfigProvider from "effect/ConfigProvider";
import * as NativeReadiness from "./NativeCodexReadiness.ts";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient } from "effect/http";
import * as Config from "../config.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Transport from "./ProviderSetupTransport.ts";
import { makeCodexChatGptAuth } from "./CodexChatGptAuth.ts";

const instanceId = ProviderInstanceId.make("qa-native-sign-in");
const layers = Layer.mergeAll(
  NodeServices.layer,
  NodePlatformCrypto.layer,
  ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: true })),
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make(() => Effect.die("Managed sign-in must never use ambient HTTP")),
  ),
  Layer.succeed(Environment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("qa-native-environment")),
  }),
);
const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.scoped,
    Effect.provide(
      Config.layerTest(process.cwd(), { prefix: "t3-native-setup-" }).pipe(
        Layer.provideMerge(layers),
      ),
    ),
  );
const fixture = Effect.gen(function* () {
  const transport = yield* Transport.make;
  const entered = yield* Queue.unbounded<void>();
  const observed = {
    ...transport,
    client: (id: ProviderInstanceId) =>
      transport
        .client(id)
        .pipe(
          Effect.map((http) =>
            http.pipe(
              HttpClient.mapRequestEffect((request) =>
                Queue.offer(entered, undefined).pipe(Effect.as(request)),
              ),
            ),
          ),
        ),
  };
  const secrets = yield* Secrets.make;
  const auth = yield* makeCodexChatGptAuth({ instanceId }).pipe(
    Effect.provideService(Secrets.ServerSecretStore, secrets),
    Effect.provideService(Transport.ProviderSetupTransport, observed),
  );
  const lease = yield* transport.begin({ instanceId });
  const next = Effect.gen(function* () {
    yield* Queue.take(entered);
    yield* TestClock.adjust("1 millis");
    const job = yield* transport.take(lease);
    assert.isNotNull(job);
    return job!;
  });
  const reply = (id: string, body: unknown) =>
    transport.complete({ ...lease, id, response: { status: 200, body: JSON.stringify(body) } });
  const phase = (value: string) =>
    auth.controller.subscribe("native-owner").pipe(
      Stream.filter((state) => state.phase === value),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );
  return { transport, auth, lease, next, reply, phase, entered };
});
const start = (f: Effect.Success<typeof fixture>) =>
  Effect.gen(function* () {
    yield* f.auth.controller.start("native-owner", Effect.void, "chatgpt", undefined, "client");
    const discovery = yield* f.next;
    assert.equal(discovery.url, "https://auth.openai.com/.well-known/openid-configuration");
    yield* f.reply(discovery.id, {
      issuer: "https://auth.openai.com",
      authorization_endpoint: "https://auth.openai.com/authorize",
      token_endpoint: "https://auth.openai.com/token",
      jwks_uri: "https://auth.openai.com/jwks",
      revocation_endpoint: "https://auth.openai.com/revoke",
    });
    const state = yield* f.phase("waiting");
    assert.isNotNull(state.authorizationUrl);
    assert.isNotNull(state.flowId);
    const authorize = new URL(state.authorizationUrl!);
    assert.equal(authorize.origin, "https://auth.openai.com");
    const callback = new URL(authorize.searchParams.get("redirect_uri")!);
    callback.search = new URLSearchParams({
      code: "QA_CODE",
      state: authorize.searchParams.get("state")!,
      client_id: "oaiapp_nativeqa",
    }).toString();
    return { state, authorize, callback };
  });
const token = (
  f: Effect.Success<typeof fixture>,
  s: Effect.Success<ReturnType<typeof start>>,
  badNonce = false,
  beforeKeys = Effect.void,
) =>
  Effect.gen(function* () {
    yield* f.auth.controller.complete("native-owner", {
      flowId: s.state.flowId!,
      callbackUrl: s.callback.toString(),
    });
    const exchange = yield* f.next;
    assert.equal(exchange.url, "https://auth.openai.com/token");
    const form = new URLSearchParams(exchange.body);
    assert.equal(form.get("resource"), "https://api.openai.com/v1");
    assert.equal(form.get("code"), "QA_CODE");
    assert.equal(form.get("redirect_uri"), s.authorize.searchParams.get("redirect_uri"));
    assert.equal(
      NodeCrypto.createHash("sha256").update(form.get("code_verifier")!).digest("base64url"),
      s.authorize.searchParams.get("code_challenge"),
    );
    const pair = NodeCrypto.generateKeyPairSync("rsa", { modulusLength: 2048 });
    const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
    const unsigned =
      encode({ alg: "RS256", kid: "qa" }) +
      "." +
      encode({
        iss: "https://auth.openai.com",
        aud: "oaiapp_nativeqa",
        sub: "qa-user",
        email: "qa@example.invalid",
        exp: 4102444800,
        nonce: badNonce ? "wrong" : s.authorize.searchParams.get("nonce"),
      });
    const jwt =
      unsigned +
      "." +
      NodeCrypto.sign("sha256", Buffer.from(unsigned), pair.privateKey).toString("base64url");
    yield* f.reply(exchange.id, {
      access_token: "QA_ACCESS",
      refresh_token: "QA_REFRESH",
      id_token: jwt,
      token_type: "Bearer",
      expires_in: 3600,
      scope: "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct",
    });
    const jwks = yield* f.next;
    assert.equal(jwks.url, "https://auth.openai.com/jwks");
    yield* beforeKeys;
    yield* f.reply(jwks.id, {
      keys: [{ ...pair.publicKey.export({ format: "jwk" }), kid: "qa", alg: "RS256", use: "sig" }],
    });
  });
it.effect(
  "completes real PKCE, signed identity verification and encrypted account persistence through native mediation",
  () =>
    provide(
      Effect.gen(function* () {
        const f = yield* fixture;
        const s = yield* start(f);
        const wrong = new URL(s.callback);
        wrong.searchParams.set("state", "wrong");
        yield* f.auth.controller
          .complete("native-owner", { flowId: s.state.flowId!, callbackUrl: wrong.toString() })
          .pipe(Effect.flip);
        assert.equal(yield* f.transport.take(f.lease), null);
        yield* token(f, s);
        const result = yield* f.phase("succeeded");
        assert.notInclude(JSON.stringify(result), "QA_ACCESS");
        assert.notInclude(JSON.stringify(result), "QA_REFRESH");
        assert.equal(Option.getOrThrow(yield* f.auth.read).email, "qa@example.invalid");
        yield* f.transport.close(f.lease);
        assert.equal((yield* f.auth.controller.modelAccess).accessToken, "QA_ACCESS");
        yield* f.auth.controller
          .complete("native-owner", { flowId: s.state.flowId!, callbackUrl: s.callback.toString() })
          .pipe(Effect.flip);
      }),
    ),
);
it.effect("allows native key verification beyond the standalone HTTP deadline", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* token(f, s, false, TestClock.adjust("6 seconds"));
      yield* f.phase("succeeded");
      assert.equal(Option.getOrThrow(yield* f.auth.read).email, "qa@example.invalid");
    }),
  ),
);
it.effect("retains identity validation for delayed keys and never stores a mismatched nonce", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* token(f, s, true, TestClock.adjust("6 seconds"));
      yield* f.phase("failed");
      assert.isTrue(Option.isNone(yield* f.auth.read));
    }),
  ),
);
it.effect("native key verification expiry denies late keys and stores no account", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* token(f, s, false, TestClock.adjust("16 seconds")).pipe(Effect.flip);
      yield* f.phase("failed");
      assert.isTrue(Option.isNone(yield* f.auth.read));
      assert.equal(yield* f.transport.take(f.lease), null);
    }),
  ),
);
it.effect(
  "native cancellation during key verification denies late keys and stores no account",
  () =>
    provide(
      Effect.gen(function* () {
        const f = yield* fixture;
        const s = yield* start(f);
        yield* token(
          f,
          s,
          false,
          f.auth.controller.cancel("native-owner", s.state.flowId!).pipe(Effect.orDie),
        ).pipe(Effect.flip);
        assert.isTrue(Option.isNone(yield* f.auth.read));
        assert.equal(yield* f.transport.take(f.lease), null);
      }),
    ),
);
it.effect("native cancellation denies a late token response and stores no account", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* f.auth.controller.complete("native-owner", {
        flowId: s.state.flowId!,
        callbackUrl: s.callback.toString(),
      });
      const exchange = yield* f.next;
      yield* f.auth.controller.cancel("native-owner", s.state.flowId!);
      yield* f.transport.close(f.lease);
      yield* f.reply(exchange.id, { access_token: "QA_LATE" }).pipe(Effect.flip);
      assert.isTrue(Option.isNone(yield* f.auth.read));
    }),
  ),
);
it.effect("requires native transport and client-mode callback before opening sign-in", () =>
  provide(
    Effect.gen(function* () {
      const secrets = yield* Secrets.make;
      yield* makeCodexChatGptAuth({ instanceId }).pipe(
        Effect.provideService(Secrets.ServerSecretStore, secrets),
        Effect.flip,
      );
      const f = yield* fixture;
      yield* f.auth.controller.start("native-owner", Effect.void, "chatgpt", undefined, "server");
      const outcome = yield* Effect.raceFirst(
        f.phase("failed").pipe(Effect.as("refused")),
        Queue.take(f.entered).pipe(Effect.as("request")),
      );
      assert.equal(outcome, "refused");
      assert.equal(yield* f.transport.take(f.lease), null);
    }),
  ),
);

const base: ServerProvider = {
  instanceId,
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: false,
  status: "warning",
  version: null,
  auth: { status: "unknown" },
  checkedAt: "2026-11-26T00:00:00.000Z",
  models: [],
  slashCommands: [],
  skills: [],
};
const proof = { revision: "e".repeat(64), version: "1.2.3" };
it.effect(
  "signed account reads its actual model catalog through the private transport with native engine proof",
  () =>
    provide(
      Effect.gen(function* () {
        const f = yield* fixture;
        const s = yield* start(f);
        yield* token(f, s);
        yield* f.phase("succeeded");
        yield* f.transport.attest({ ...f.lease, engine: proof });
        const attempt = yield* NativeReadiness.check({
          base,
          auth: f.auth,
          engine: f.transport.engine(instanceId),
        }).pipe(Effect.forkChild);
        const job = yield* Effect.raceFirst(
          f.next,
          Fiber.join(attempt).pipe(
            Effect.andThen(Effect.die("Readiness finished before its model request")),
          ),
        );
        assert.equal(job.url, "https://api.openai.com/v1/models");
        assert.equal(job.headers.authorization, "Bearer QA_ACCESS");
        yield* f.reply(job.id, {
          models: [
            { slug: "qa-model", display_name: "QA model", visibility: "list" },
            { slug: "hidden", display_name: "Hidden", visibility: "hide" },
          ],
        });
        const result = yield* Fiber.join(attempt);
        assert.equal(result.status, "ready");
        assert.isTrue(result.installed);
        assert.equal(result.version, "1.2.3");
        assert.deepEqual(
          result.models.map((x) => x.slug),
          ["qa-model"],
        );
        assert.deepEqual(result.supportedRuntimeModes, ["approval-required"]);
        assert.equal(result.auth.status, "authenticated");
        assert.notInclude(JSON.stringify(result), "QA_ACCESS");
        assert.notInclude(JSON.stringify(result), "QA_REFRESH");
        assert.notInclude(JSON.stringify(result), "qa@example.invalid");
      }),
    ),
);
it.effect("a missing native proof refuses before using an authenticated account", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* token(f, s);
      yield* f.phase("succeeded");
      const result = yield* NativeReadiness.check({
        base,
        auth: f.auth,
        engine: f.transport.engine(instanceId),
      });
      assert.equal(result.status, "warning");
      assert.isFalse(result.installed);
      assert.deepEqual(result.models, []);
      assert.equal(yield* f.transport.take(f.lease), null);
    }),
  ),
);
it.effect("a replacement readiness attempt withholds a completed model catalog", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* token(f, s);
      yield* f.phase("succeeded");
      yield* f.transport.attest({ ...f.lease, engine: proof });
      const attempt = yield* NativeReadiness.check({
        base,
        auth: f.auth,
        engine: f.transport.engine(instanceId),
      }).pipe(Effect.forkChild);
      const job = yield* Effect.raceFirst(
        f.next,
        Fiber.join(attempt).pipe(
          Effect.andThen(Effect.die("Readiness finished before its model request")),
        ),
      );
      yield* f.transport.attest({ ...f.lease, engine: proof });
      yield* f.reply(job.id, {
        models: [{ slug: "qa-model", display_name: "QA model", visibility: "list" }],
      });
      const result = yield* Fiber.join(attempt);
      assert.equal(result.status, "warning");
      assert.deepEqual(result.models, []);
    }),
  ),
);
it.effect("empty inventory and a changed account cannot claim a usable endpoint", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* token(f, s);
      yield* f.phase("succeeded");
      yield* f.transport.attest({ ...f.lease, engine: proof });
      for (const changed of [false, true]) {
        let reads = 0;
        const auth = {
          ...f.auth,
          read: Effect.suspend(() => (changed && ++reads > 1 ? Effect.succeedNone : f.auth.read)),
        };
        const attempt = yield* NativeReadiness.check({
          base,
          auth,
          engine: f.transport.engine(instanceId),
        }).pipe(Effect.forkChild);
        const job = yield* Effect.raceFirst(
          f.next,
          Fiber.join(attempt).pipe(
            Effect.andThen(Effect.die("Readiness finished before its model request")),
          ),
        );
        yield* f.reply(job.id, {
          models: changed
            ? [{ slug: "qa-model", display_name: "QA model", visibility: "list" }]
            : [],
        });
        const result = yield* Fiber.join(attempt);
        assert.equal(result.status, "warning");
        assert.deepEqual(result.models, []);
      }
    }),
  ),
);

it.effect("an account without token sharing cannot request a native model catalog", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const s = yield* start(f);
      yield* token(f, s);
      yield* f.phase("succeeded");
      yield* f.transport.attest({ ...f.lease, engine: proof });
      const auth = {
        ...f.auth,
        read: f.auth.read.pipe(Effect.map(Option.map((record) => ({ ...record, scopes: [] })))),
      };
      const attempt = yield* NativeReadiness.check({
        base,
        auth,
        engine: f.transport.engine(instanceId),
      }).pipe(Effect.forkChild);
      const observed = yield* Effect.raceFirst(
        Fiber.join(attempt).pipe(Effect.map((result) => ({ result, unexpected: false }))),
        f.next.pipe(Effect.map(() => ({ result: base, unexpected: true }))),
      );
      assert.isFalse(observed.unexpected);
      const result = observed.result;
      assert.equal(result.status, "warning");
      assert.equal(result.auth.status, "unauthenticated");
      assert.deepEqual(result.models, []);
      assert.equal(yield* f.transport.take(f.lease), null);
    }),
  ),
);
