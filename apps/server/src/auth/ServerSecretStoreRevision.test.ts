import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as PlatformError from "effect/PlatformError";
import * as Option from "effect/Option";
import * as ServerConfig from "../config.ts";
import * as ServerSecretStore from "./ServerSecretStore.ts";
import * as ProviderCredentialStore from "../provider/ProviderCredentialStore.ts";

const config = () => ServerConfig.layerTest(process.cwd(), { prefix: "t3-revision-test-" });

it.layer(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))("credential revisions", (it) => {
  it.effect("binds revisions to a key, store lifetime and every credential mutation", () =>
    Effect.gen(function* () {
      const store = yield* ServerSecretStore.make;
      const read = store.revision!;
      assert.isTrue(Option.isNone(yield* read("a")));
      const bytes = new TextEncoder().encode("synthetic credential only");
      yield* store.set("a", bytes);
      const first = Option.getOrThrow(yield* read("a"));
      assert.match(first, /^[a-f0-9]{64}$/);
      assert.equal(Option.getOrThrow(yield* read("a")), first);
      yield* store.set("b", bytes);
      assert.notEqual(Option.getOrThrow(yield* read("b")), first);
      assert.equal(Option.getOrThrow(yield* read("a")), first);
      yield* store.set("a", bytes);
      const second = Option.getOrThrow(yield* read("a"));
      assert.notEqual(second, first);
      yield* store.remove("a");
      assert.isTrue(Option.isNone(yield* read("a")));
      yield* store.create("a", bytes);
      assert.notEqual(Option.getOrThrow(yield* read("a")), second);
      const restarted = yield* ServerSecretStore.make;
      assert.notEqual(
        Option.getOrThrow(yield* restarted.revision!("a")),
        Option.getOrThrow(yield* read("a")),
      );
      yield* store.set("a", new Uint8Array());
      assert.isTrue(Option.isNone(yield* read("a")));
      const error = yield* Effect.flip(read("../outside"));
      assert.instanceOf(error, ServerSecretStore.SecretStoreNameError);
    }).pipe(Effect.provide(config())),
  );

  it.effect("retires failed creates and reflects current external replacement", () =>
    Effect.gen(function* () {
      const store = yield* ServerSecretStore.make;
      const fs = yield* FileSystem.FileSystem;
      const bytes = new Uint8Array([1, 2, 3]);
      yield* store.create("a", bytes);
      const before = Option.getOrThrow(yield* store.revision!("a"));
      yield* Effect.flip(store.create("a", new Uint8Array([9])));
      assert.deepEqual(Option.getOrThrow(yield* store.get("a")), bytes);
      const after = Option.getOrThrow(yield* store.revision!("a"));
      assert.notEqual(after, before);
      yield* fs.writeFile(store.directory + "/a.bin", new Uint8Array([9, 8, 7]));
      assert.notEqual(Option.getOrThrow(yield* store.revision!("a")), after);
    }).pipe(Effect.provide(config())),
  );

  it.effect("withholds a revision when a write overlaps its read", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let holdRead = false;
      const store = yield* ServerSecretStore.make.pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          readFile: (path) =>
            Effect.gen(function* () {
              const bytes = yield* fs.readFile(path);
              if (holdRead) {
                holdRead = false;
                yield* Deferred.succeed(entered, void 0);
                yield* Deferred.await(release);
              }
              return bytes;
            }),
        }),
      );
      yield* store.set("a", new Uint8Array([1]));
      holdRead = true;
      const pending = yield* store.revision!("a").pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* store.set("a", new Uint8Array([2]));
      yield* Deferred.succeed(release, void 0);
      assert.isTrue(Option.isNone(yield* Fiber.join(pending)));
      assert.isTrue(Option.isSome(yield* store.revision!("a")));
    }).pipe(Effect.provide(config())),
  );

  it.effect("retires interrupted writes and withholds revisions while they are pending", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const entered = yield* Deferred.make<void>();
      let holdWrite = false;
      const store = yield* ServerSecretStore.make.pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          rename: (from, to) =>
            holdWrite
              ? Deferred.succeed(entered, void 0).pipe(Effect.andThen(Effect.never))
              : fs.rename(from, to),
        }),
      );
      yield* store.set("a", new Uint8Array([1]));
      const before = Option.getOrThrow(yield* store.revision!("a"));
      holdWrite = true;
      const pending = yield* store.set("a", new Uint8Array([2])).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      assert.isTrue(Option.isNone(yield* store.revision!("a")));
      yield* Fiber.interrupt(pending);
      holdWrite = false;
      assert.notEqual(Option.getOrThrow(yield* store.revision!("a")), before);
      assert.deepEqual(Option.getOrThrow(yield* store.get("a")), new Uint8Array([1]));
    }).pipe(Effect.provide(config())),
  );

  it.effect("retires previous revisions after a failed read recovers", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let refuse = false;
      const store = yield* ServerSecretStore.make.pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          readFile: (path) =>
            refuse
              ? Effect.fail(
                  PlatformError.systemError({
                    _tag: "PermissionDenied",
                    module: "FileSystem",
                    method: "readFile",
                    pathOrDescriptor: String(path),
                    description: "synthetic refusal",
                  }),
                )
              : fs.readFile(path),
        }),
      );
      yield* store.set("a", new Uint8Array([1]));
      const before = Option.getOrThrow(yield* store.revision!("a"));
      refuse = true;
      assert.instanceOf(
        yield* Effect.flip(store.revision!("a")),
        ServerSecretStore.SecretStoreReadError,
      );
      refuse = false;
      assert.notEqual(Option.getOrThrow(yield* store.revision!("a")), before);
    }).pipe(Effect.provide(config())),
  );

  it.effect("reads a fresh revision through the actual provider binding", () =>
    Effect.gen(function* () {
      const store = yield* ServerSecretStore.make;
      const bound = yield* ProviderCredentialStore.make("cursor", "personal").pipe(
        Effect.provideService(ServerSecretStore.ServerSecretStore, store),
      );
      yield* bound.set(new Uint8Array([1]));
      const before = Option.getOrThrow(yield* bound.revision);
      assert.equal(before, Option.getOrThrow(yield* store.revision!(bound.binding.key)));
      yield* bound.set(new Uint8Array([1]));
      assert.notEqual(Option.getOrThrow(yield* bound.revision), before);
      yield* bound.remove;
      assert.isTrue(Option.isNone(yield* bound.revision));
      const { revision: _revision, ...unsupported } = store;
      const unavailable = yield* ProviderCredentialStore.make("cursor", "personal").pipe(
        Effect.provideService(ServerSecretStore.ServerSecretStore, unsupported),
      );
      assert.isTrue(Option.isNone(yield* unavailable.revision));
    }).pipe(Effect.provide(config())),
  );
});
