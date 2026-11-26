import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as HttpClient from "effect/http/HttpClient";
import * as Config from "../config.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Credential from "./ProviderCredentialStore.ts";
import { makeCodexChatGptAuth } from "./CodexChatGptAuth.ts";

const instanceId = ProviderInstanceId.make("qa-model-account");
const record = {
  clientId: "oaiapp_fixture",
  accessToken: "QA_SYNTHETIC_ACCESS",
  refreshToken: "QA_NEVER_RENEW",
  expiresAt: Number.MAX_SAFE_INTEGER,
  earliestRefreshAt: null,
  scopes: ["chatgpt.tokens.use.direct"],
  subject: "qa",
  email: null,
};
const fixture = Effect.gen(function* () {
  const secrets = yield* Secrets.make;
  const credential = yield* Credential.make("codex-chatgpt", instanceId).pipe(
    Effect.provideService(Secrets.ServerSecretStore, secrets),
  );
  const save = (value: unknown) => credential.set(new TextEncoder().encode(JSON.stringify(value)));
  yield* save(record);
  const auth = yield* makeCodexChatGptAuth({ instanceId }).pipe(
    Effect.provideService(Secrets.ServerSecretStore, secrets),
  );
  return { auth, secrets, credential, save };
});
const layers = Layer.mergeAll(
  NodeServices.layer,
  NodeCrypto.layer,
  Layer.succeed(
    HttpClient.HttpClient,
    HttpClient.make(() => Effect.die("Account access must not use network.")),
  ),
  Layer.succeed(Environment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("qa-environment")),
  }),
);
const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.scoped,
    Effect.provide(
      Config.layerTest(process.cwd(), { prefix: "t3-model-access-" }).pipe(
        Layer.provideMerge(layers),
      ),
    ),
  );
it.effect("reads the active managed account without renewal or a network request", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = yield* f.credential.revision;
      assert.deepEqual(yield* f.auth.controller.modelAccess, {
        accessToken: record.accessToken,
        expiresAt: record.expiresAt,
        kind: "chatgpt-managed",
        resource: "https://api.openai.com/v1",
      });
      assert.deepEqual(yield* f.credential.revision, before);
      yield* f.save({
        activeClientId: "other",
        sessions: [record, { ...record, clientId: "other", accessToken: "QA_OTHER" }],
      });
      assert.equal((yield* f.auth.controller.modelAccess).accessToken, "QA_OTHER");
    }),
  ),
);
it.effect("refuses expired, ambiguous, unshared or malformed credentials without renewal", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      for (const value of [
        { ...record, expiresAt: 0 },
        { ...record, scopes: [] },
        { ...record, accessToken: "line\nbreak" },
        { ...record, accessToken: "trailing\n" },
        { ...record, accessToken: "x".repeat(32769) },
        { activeClientId: "missing", sessions: [record] },
        { activeClientId: record.clientId, sessions: [record, record] },
      ]) {
        yield* f.save(value);
        const before = yield* f.credential.revision;
        const error = yield* Effect.flip(f.auth.controller.modelAccess);
        assert.equal(error._tag, "ProviderSetupError");
        assert.isFalse(error.message.includes(record.refreshToken));
        assert.deepEqual(yield* f.credential.revision, before);
      }
    }),
  ),
);
it.effect("refuses a credential read overlapping a revision change", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      let reads = 0;
      const changed = {
        ...f.secrets,
        revision: () => Effect.sync(() => Option.some(String(++reads))),
      };
      const auth = yield* makeCodexChatGptAuth({ instanceId }).pipe(
        Effect.provideService(Secrets.ServerSecretStore, changed),
      );
      assert.equal((yield* Effect.flip(auth.controller.modelAccess))._tag, "ProviderSetupError");
    }),
  ),
);
it.effect("refuses stores without revision support and a different configured resource", () =>
  provide(
    Effect.gen(function* () {
      const f = yield* fixture;
      const { revision: _revision, ...unversioned } = f.secrets;
      const auth = yield* makeCodexChatGptAuth({ instanceId }).pipe(
        Effect.provideService(Secrets.ServerSecretStore, unversioned),
      );
      assert.equal((yield* Effect.flip(auth.controller.modelAccess))._tag, "ProviderSetupError");
      const other = yield* makeCodexChatGptAuth({
        instanceId,
        resource: "https://other.invalid",
      }).pipe(Effect.provideService(Secrets.ServerSecretStore, f.secrets));
      assert.equal((yield* Effect.flip(other.controller.modelAccess))._tag, "ProviderSetupError");
    }),
  ),
);
