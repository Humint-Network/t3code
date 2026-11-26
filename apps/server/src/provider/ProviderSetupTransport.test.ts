import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Exit from "effect/Exit";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import { HttpClient, HttpClientRequest } from "effect/http";
import * as Transport from "./ProviderSetupTransport.ts";

const instanceId = ProviderInstanceId.make("qa-setup");
const discovery = "https://auth.openai.com/.well-known/openid-configuration";
const provide = <A, E, R>(effect: Effect.Effect<A, E, R>, managed = true) =>
  effect.pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        NodeCrypto.layer,
        ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: managed })),
      ),
    ),
  );
const fixture = Effect.gen(function* () {
  const transport = yield* Transport.make;
  const http = yield* transport.client(instanceId);
  const lease = yield* transport.begin({ instanceId });
  return { transport, http, lease };
});
const pending = (f: Effect.Success<typeof fixture>, request = HttpClientRequest.get(discovery)) =>
  Effect.gen(function* () {
    const fiber = yield* f.http.execute(request).pipe(Effect.forkChild);
    yield* TestClock.adjust("1 millis");
    const job = yield* f.transport.take(f.lease);
    assert.isNotNull(job);
    return { fiber, job: job! };
  });
it.effect("mediates discovery and token exchange once without an external HTTP client", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const p = yield* pending(f);
      assert.equal(p.job.url, discovery);
      assert.equal(p.job.instanceId, instanceId);
      assert.equal(yield* f.transport.take(f.lease), null);
      yield* f.transport.complete({
        ...f.lease,
        id: p.job.id,
        response: { status: 200, body: '{"issuer":"https://auth.openai.com"}' },
      });
      assert.deepEqual(yield* (yield* Fiber.join(p.fiber)).json, {
        issuer: "https://auth.openai.com",
      });
      yield* f.transport
        .complete({ ...f.lease, id: p.job.id, response: { status: 200, body: "{}" } })
        .pipe(Effect.flip);
      const token = yield* pending(
        f,
        HttpClientRequest.post("https://auth.openai.com/token").pipe(
          HttpClientRequest.bodyText(
            "code=QA_CODE&code_verifier=QA_VERIFIER",
            "application/x-www-form-urlencoded",
          ),
        ),
      );
      assert.equal(token.job.body, "code=QA_CODE&code_verifier=QA_VERIFIER");
      assert.deepEqual(token.job.headers, { "content-type": "application/x-www-form-urlencoded" });
      yield* f.transport.complete({
        ...f.lease,
        id: token.job.id,
        response: { status: 400, body: '{"error":"invalid_grant"}' },
      });
      assert.equal((yield* Fiber.join(token.fiber)).status, 400);
    }),
  ),
);
it.effect(
  "requires an explicit lease and refuses unknown instances, duplicate setup and standalone mode",
  () =>
    provide(
      Effect.gen(function* () {
        const transport = yield* Transport.make;
        yield* transport.begin({ instanceId }).pipe(Effect.flip);
        const http = yield* transport.client(instanceId);
        yield* http.get(discovery).pipe(Effect.flip);
        const lease = yield* transport.begin({ instanceId });
        yield* transport.begin({ instanceId }).pipe(Effect.flip);
        yield* transport.close(lease);
        yield* transport.close(lease);
        yield* http.get(discovery).pipe(Effect.flip);
      }),
    ),
);
it.effect("refuses all setup transport in standalone mode", () =>
  provide(
    Effect.gen(function* () {
      const transport = yield* Transport.make;
      yield* transport.client(instanceId).pipe(Effect.flip);
      yield* transport.begin({ instanceId }).pipe(Effect.flip);
      yield* transport.take({ lease: "a".repeat(64) }).pipe(Effect.flip);
      yield* transport.close({ lease: "a".repeat(64) }).pipe(Effect.flip);
    }),
    false,
  ),
);
it.effect("cancellation removes the request and cannot accept a late reply", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const p = yield* pending(f);
      yield* Fiber.interrupt(p.fiber);
      assert.equal(yield* f.transport.take(f.lease), null);
      yield* f.transport
        .complete({ ...f.lease, id: p.job.id, response: { status: 200, body: "{}" } })
        .pipe(Effect.flip);
    }),
  ),
);
it.effect(
  "closing setup rejects in-flight requests and withholds old replies after reopening",
  () =>
    provide(
      Effect.gen(function* () {
        const f = yield* fixture;
        const p = yield* pending(f);
        yield* f.transport.close(f.lease);
        yield* Fiber.join(p.fiber).pipe(Effect.flip);
        const next = yield* f.transport.begin({ instanceId });
        assert.notEqual(next.lease, f.lease.lease);
        yield* f.transport
          .complete({ ...next, id: p.job.id, response: { status: 200, body: "{}" } })
          .pipe(Effect.flip);
        yield* f.transport.take(f.lease).pipe(Effect.flip);
      }),
    ),
);
it.effect(
  "replacement auth controllers retire old leases and cannot use the new controller's lease",
  () =>
    provide(
      Effect.gen(function* () {
        const f = yield* fixture;
        const p = yield* pending(f);
        const next = yield* f.transport.client(instanceId);
        yield* Fiber.join(p.fiber).pipe(Effect.flip);
        yield* f.transport.begin({ instanceId });
        yield* f.http.get(discovery).pipe(Effect.flip);
        const fiber = yield* next.get(discovery).pipe(Effect.forkChild);
        yield* TestClock.adjust("1 millis");
        yield* Fiber.interrupt(fiber);
      }),
    ),
);
it.effect("expires leases and reclaims their capacity", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* TestClock.adjust("10 minutes");
      yield* f.transport.take(f.lease).pipe(Effect.flip);
      yield* f.http.get(discovery).pipe(Effect.flip);
      yield* f.transport.begin({ instanceId });
    }),
  ),
);
it.effect("bounds each request wait and refuses replies after timeout", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const p = yield* pending(f);
      yield* TestClock.adjust("15 seconds");
      yield* f.transport
        .complete({ ...f.lease, id: p.job.id, response: { status: 200, body: "{}" } })
        .pipe(Effect.flip);
      yield* Fiber.join(p.fiber).pipe(Effect.flip);
    }),
  ),
);
it.effect("bounds active instances and pending requests", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      for (let i = 0; i < 3; i++) {
        const id = ProviderInstanceId.make(`qa-${i}`);
        yield* f.transport.client(id);
        yield* f.transport.begin({ instanceId: id });
      }
      const extra = ProviderInstanceId.make("extra");
      yield* f.transport.client(extra);
      yield* f.transport.begin({ instanceId: extra }).pipe(Effect.flip);
      const jobs = [];
      for (let i = 0; i < 4; i++) jobs.push(yield* pending(f));
      yield* f.http.get(discovery).pipe(Effect.flip);
      for (const job of jobs) yield* Fiber.interrupt(job.fiber);
    }),
  ),
);
it.effect("keeps requests and completions bound to their exact provider lease", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const p = yield* pending(f);
      const id = ProviderInstanceId.make("different");
      yield* f.transport.client(id);
      const other = yield* f.transport.begin({ instanceId: id });
      assert.equal(yield* f.transport.take(other), null);
      yield* f.transport
        .complete({ ...other, id: p.job.id, response: { status: 200, body: "{}" } })
        .pipe(Effect.flip);
      yield* f.transport.complete({ ...f.lease, id: p.job.id, response: null });
      yield* Fiber.join(p.fiber).pipe(Effect.flip);
    }),
  ),
);
it.effect("admits only the fixed OAuth and model-catalog request shapes", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const bad = [
        HttpClientRequest.get("http://auth.openai.com/token"),
        HttpClientRequest.get("https://other.invalid/token"),
        HttpClientRequest.get("https://api.openai.com/v1/responses"),
        HttpClientRequest.get(discovery + "?code=private"),
        HttpClientRequest.get(discovery + "#private"),
        HttpClientRequest.get("https://secret@auth.openai.com/token"),
        HttpClientRequest.get(discovery).pipe(HttpClientRequest.setHeader("cookie", "QA_SECRET")),
        HttpClientRequest.get(discovery).pipe(HttpClientRequest.bearerToken("QA_SECRET")),
        HttpClientRequest.get(discovery).pipe(HttpClientRequest.setHeader("host", "other.invalid")),
        HttpClientRequest.get(discovery).pipe(HttpClientRequest.setUrlParam("q", "QA_SECRET")),
        HttpClientRequest.put("https://auth.openai.com/token"),
        HttpClientRequest.post("https://auth.openai.com/token").pipe(
          HttpClientRequest.bodyText("{}", "application/json"),
        ),
        HttpClientRequest.post("https://auth.openai.com/token").pipe(
          HttpClientRequest.bodyText("x".repeat(65537), "application/x-www-form-urlencoded"),
        ),
        HttpClientRequest.get(discovery).pipe(HttpClientRequest.bodyText("body")),
        HttpClientRequest.get("https://api.openai.com/v1/models"),
        HttpClientRequest.post("https://api.openai.com/v1/models").pipe(
          HttpClientRequest.bearerToken("QA_SECRET"),
        ),
      ];
      for (const request of bad) {
        const attempt = yield* f.http.execute(request).pipe(Effect.forkChild);
        yield* TestClock.adjust("1 millis");
        assert.equal(yield* f.transport.take(f.lease), null);
        const error = yield* Fiber.join(attempt).pipe(Effect.flip);
        assert.notInclude(JSON.stringify(error), "QA_SECRET");
        assert.notInclude(String(error), "private");
        assert.equal(yield* f.transport.take(f.lease), null);
      }
      const catalog = yield* pending(
        f,
        HttpClientRequest.get("https://api.openai.com/v1/models").pipe(
          HttpClientRequest.bearerToken("QA_SECRET"),
        ),
      );
      assert.equal(catalog.job.headers.authorization, "Bearer QA_SECRET");
      yield* f.transport.complete({
        ...f.lease,
        id: catalog.job.id,
        response: { status: 200, body: '{"models":[]}' },
      });
      assert.deepEqual(yield* (yield* Fiber.join(catalog.fiber)).json, { models: [] });
    }),
  ),
);
it.effect(
  "rejects malformed input, oversized UTF-8 replies and redirects without consuming a valid response",
  () =>
    provide(
      Effect.gen(function* () {
        const f = yield* fixture;
        const p = yield* pending(f);
        for (const response of [
          { status: 302, body: "" },
          { status: 200, body: "é".repeat(524289) },
          { status: 600, body: "" },
        ])
          yield* f.transport.complete({ ...f.lease, id: p.job.id, response }).pipe(Effect.flip);
        yield* f.transport.take({ ...f.lease, extra: true } as never).pipe(Effect.flip);
        yield* f.transport.complete({
          ...f.lease,
          id: p.job.id,
          response: { status: 200, body: "{}" },
        });
        assert.equal((yield* Fiber.join(p.fiber)).status, 200);
      }),
    ),
);

it.effect("withholds a response when the lease expires between completion and release", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const clock = yield* Clock.Clock;
      let expired = false;
      const http = f.http.pipe(
        HttpClient.transform((effect) =>
          effect.pipe(
            Effect.provideService(Clock.Clock, {
              ...clock,
              currentTimeMillis: Effect.sync(() => (expired ? 600_000 : 0)),
            }),
          ),
        ),
      );
      const p = yield* pending({ ...f, http });
      expired = true;
      yield* f.transport.complete({
        ...f.lease,
        id: p.job.id,
        response: { status: 200, body: "{}" },
      });
      yield* Fiber.join(p.fiber).pipe(Effect.flip);
    }),
  ),
);

it.effect(
  "binds immutable native engine proof and correlated outcomes to the current instance generation",
  () =>
    provide(
      Effect.gen(function* () {
        const f = yield* fixture;
        yield* f.transport.engine(instanceId).pipe(
          Effect.exit,
          Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
        );
        const proof = { revision: "e".repeat(64), version: "1.2.3" };
        const first = yield* f.transport.attest({ ...f.lease, engine: proof });
        assert.equal(first.phase, "pending");
        assert.equal((yield* f.transport.engine(instanceId)).revision, proof.revision);
        assert.deepEqual(yield* f.transport.status({ ...f.lease, check: first.check }), first);
        for (const engine of [
          { ...proof, revision: "a".repeat(64) },
          { ...proof, version: "2" },
          { ...proof, version: "1\nprivate" },
        ])
          yield* f.transport.attest({ ...f.lease, engine }).pipe(
            Effect.exit,
            Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
          );
        const next = yield* f.transport.attest({ ...f.lease, engine: proof });
        assert.notEqual(first.check, next.check);
        assert.isFalse(yield* f.transport.settle(instanceId, first.check, true));
        yield* f.transport.status({ ...f.lease, check: first.check }).pipe(
          Effect.exit,
          Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
        );
        assert.equal(
          (yield* f.transport.status({ ...f.lease, check: next.check })).phase,
          "pending",
        );
        assert.isTrue(yield* f.transport.settle(instanceId, next.check, true));
        assert.isFalse(yield* f.transport.settle(instanceId, next.check, false));
        assert.equal((yield* f.transport.status({ ...f.lease, check: next.check })).phase, "ready");
        yield* f.transport.close(f.lease);
        yield* f.transport.status({ ...f.lease, check: next.check }).pipe(
          Effect.exit,
          Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
        );
        assert.equal((yield* f.transport.engine(instanceId)).revision, proof.revision);
        yield* f.transport.client(instanceId);
        yield* f.transport.engine(instanceId).pipe(
          Effect.exit,
          Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
        );
        assert.isFalse(yield* f.transport.settle(instanceId, next.check, true));
      }),
    ),
);
it.effect("rejects wrong lease, extra fields, expired setup and standalone attestation", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const engine = { revision: "e".repeat(64), version: "1" };
      yield* f.transport.attest({ lease: "a".repeat(64), engine }).pipe(
        Effect.exit,
        Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
      );
      yield* f.transport.attest({ ...f.lease, engine, extra: true } as never).pipe(
        Effect.exit,
        Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
      );
      yield* f.transport
        .attest({ ...f.lease, engine: { ...engine, path: "/bin/sh" } } as never)
        .pipe(
          Effect.exit,
          Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
        );
      yield* TestClock.adjust("10 minutes");
      yield* f.transport.attest({ ...f.lease, engine }).pipe(
        Effect.exit,
        Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
      );
    }),
  ),
);
it.effect("standalone transport has no native engine trust path", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* Transport.make;
      yield* f.engine(instanceId).pipe(
        Effect.exit,
        Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
      );
      yield* f
        .attest({ lease: "a".repeat(64), engine: { revision: "e".repeat(64), version: "1" } })
        .pipe(
          Effect.exit,
          Effect.tap((exit) => Effect.sync(() => assert.isTrue(Exit.isFailure(exit)))),
        );
      assert.isFalse(yield* f.settle(instanceId, "b".repeat(64), true));
    }),
    false,
  ),
);
