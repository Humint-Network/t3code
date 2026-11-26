import * as Context from "effect/Context";
import * as Mac from "effect/crypto/Mac";
import * as Hex from "effect/encoding/Hex";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as ServerConfig from "../config.ts";

const secretStoreErrorContext = {
  resource: Schema.String,
  cause: Schema.Defect(),
};

export class SecretStoreSecureError extends Schema.TaggedError<SecretStoreSecureError>()(
  "SecretStoreSecureError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to secure ${this.resource}.`;
  }
}

export class SecretStoreNameError extends Schema.TaggedError<SecretStoreNameError>()(
  "SecretStoreNameError",
  {
    resource: Schema.String,
  },
) {
  override get message(): string {
    return `Refused ${this.resource}: a secret name cannot contain a path separator.`;
  }
}

export class SecretStoreReadError extends Schema.TaggedError<SecretStoreReadError>()(
  "SecretStoreReadError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to read ${this.resource}.`;
  }
}

export class SecretStoreTemporaryPathError extends Schema.TaggedError<SecretStoreTemporaryPathError>()(
  "SecretStoreTemporaryPathError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to create temporary path for ${this.resource}.`;
  }
}

export class SecretStorePersistError extends Schema.TaggedError<SecretStorePersistError>()(
  "SecretStorePersistError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to persist ${this.resource}.`;
  }
}

export class SecretStoreRandomGenerationError extends Schema.TaggedError<SecretStoreRandomGenerationError>()(
  "SecretStoreRandomGenerationError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to generate random bytes for ${this.resource}.`;
  }
}

export class SecretStoreConcurrentReadError extends Schema.TaggedError<SecretStoreConcurrentReadError>()(
  "SecretStoreConcurrentReadError",
  {
    resource: Schema.String,
  },
) {
  override get message(): string {
    return `Failed to read ${this.resource} after concurrent creation.`;
  }
}

export class SecretStoreRemoveError extends Schema.TaggedError<SecretStoreRemoveError>()(
  "SecretStoreRemoveError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to remove ${this.resource}.`;
  }
}

export class SecretStoreDecodeError extends Schema.TaggedError<SecretStoreDecodeError>()(
  "SecretStoreDecodeError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to decode ${this.resource}.`;
  }
}

export class SecretStoreEncodeError extends Schema.TaggedError<SecretStoreEncodeError>()(
  "SecretStoreEncodeError",
  {
    ...secretStoreErrorContext,
  },
) {
  override get message(): string {
    return `Failed to encode ${this.resource}.`;
  }
}

export const SecretStoreError = Schema.Union([
  SecretStoreSecureError,
  SecretStoreNameError,
  SecretStoreReadError,
  SecretStoreTemporaryPathError,
  SecretStorePersistError,
  SecretStoreRandomGenerationError,
  SecretStoreConcurrentReadError,
  SecretStoreRemoveError,
  SecretStoreDecodeError,
  SecretStoreEncodeError,
]);
export type SecretStoreError = typeof SecretStoreError.Type;
export const isSecretStoreError = Schema.is(SecretStoreError);

const isPlatformError = (value: unknown): value is PlatformError.PlatformError =>
  Predicate.isTagged(value, "PlatformError");

export const isSecretAlreadyExistsError = (error: SecretStoreError): boolean =>
  "cause" in error && isPlatformError(error.cause) && error.cause.reason._tag === "AlreadyExists";

export class ServerSecretStore extends Context.Service<
  ServerSecretStore,
  {
    readonly directory?: string;
    readonly revision?: (name: string) => Effect.Effect<Option.Option<string>, SecretStoreError>;
    readonly get: (name: string) => Effect.Effect<Option.Option<Uint8Array>, SecretStoreError>;
    readonly set: (name: string, value: Uint8Array) => Effect.Effect<void, SecretStoreError>;
    readonly create: (name: string, value: Uint8Array) => Effect.Effect<void, SecretStoreError>;
    readonly getOrCreateRandom: (
      name: string,
      bytes: number,
    ) => Effect.Effect<Uint8Array, SecretStoreError>;
    readonly remove: (name: string) => Effect.Effect<void, SecretStoreError>;
  }
>()("t3/auth/ServerSecretStore") {}

/** @public */
export const make = Effect.gen(function* () {
  const crypto = yield* SecureRandom.SecureRandom;
  const mac = yield* Mac.Mac;
  const revisionKey = yield* mac.generateKey(Mac.hmacSha256);
  const generations = new Map<string, { value: bigint; pending: number }>();
  const generation = (name: string) => {
    let state = generations.get(name);
    if (state === undefined) {
      state = { value: 0n, pending: 0 };
      generations.set(name, state);
    }
    return state;
  };
  const mutating = <A, E, R>(name: string, operation: Effect.Effect<A, E, R>) =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        const state = generation(name);
        state.value += 1n;
        state.pending += 1;
        return state;
      }),
      () => operation,
      (state) =>
        Effect.sync(() => {
          state.value += 1n;
          state.pending -= 1;
        }),
    );
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;

  yield* fileSystem.makeDirectory(serverConfig.secretsDir, { recursive: true });
  yield* fileSystem.chmod(serverConfig.secretsDir, 0o700).pipe(
    Effect.mapError(
      (cause) =>
        new SecretStoreSecureError({
          resource: `secrets directory ${serverConfig.secretsDir}`,
          cause,
        }),
    ),
  );

  // Some names carry text from outside the server; one with a separator would leave the directory.
  const resolveSecretPath = (name: string): Effect.Effect<string, SecretStoreNameError> =>
    /[/\\\0]/.test(name)
      ? Effect.fail(new SecretStoreNameError({ resource: `secret ${JSON.stringify(name)}` }))
      : Effect.succeed(path.join(serverConfig.secretsDir, `${name}.bin`));
  const emptySecretRepair = yield* Semaphore.make(1);
  const isMissingOrNonEmpty = (secret: Option.Option<Uint8Array>) =>
    Option.isNone(secret) || secret.value.byteLength > 0;

  const get: ServerSecretStore["Service"]["get"] = (name) =>
    resolveSecretPath(name).pipe(
      Effect.flatMap((secretPath) =>
        fileSystem.readFile(secretPath).pipe(
          Effect.map((bytes) => Option.some(Uint8Array.from(bytes))),
          Effect.catch((cause) =>
            cause.reason._tag === "NotFound"
              ? Effect.succeedNone
              : Effect.fail(
                  new SecretStoreReadError({
                    resource: `secret ${name}`,
                    cause,
                  }),
                ),
          ),
        ),
      ),
      Effect.withSpan("ServerSecretStore.get"),
    );

  const set: ServerSecretStore["Service"]["set"] = (name, value) =>
    resolveSecretPath(name).pipe(
      Effect.flatMap((secretPath) => mutating(name, setAt(name, secretPath, value))),
      Effect.withSpan("ServerSecretStore.set"),
    );

  const setAt = (name: string, secretPath: string, value: Uint8Array) =>
    crypto.uuidV4().pipe(
      Effect.mapError(
        (cause) =>
          new SecretStoreTemporaryPathError({
            resource: `secret ${name}`,
            cause,
          }),
      ),
      Effect.flatMap((uuid) => {
        const tempPath = `${secretPath}.${uuid}.tmp`;
        return Effect.gen(function* () {
          yield* fileSystem.writeFile(tempPath, value);
          yield* fileSystem.chmod(tempPath, 0o600);
          yield* fileSystem.rename(tempPath, secretPath);
          yield* fileSystem.chmod(secretPath, 0o600);
        }).pipe(
          Effect.catch((cause) =>
            fileSystem.remove(tempPath).pipe(
              Effect.ignore,
              Effect.flatMap(() =>
                Effect.fail(
                  new SecretStorePersistError({
                    resource: `secret ${name}`,
                    cause,
                  }),
                ),
              ),
            ),
          ),
        );
      }),
    );

  // `link` publishes the fully written temp file under the final name, or fails with
  // AlreadyExists, so a concurrent reader never observes a partially written secret.
  const create: ServerSecretStore["Service"]["create"] = (name, value) =>
    resolveSecretPath(name).pipe(
      Effect.flatMap((secretPath) => mutating(name, createAt(name, secretPath, value))),
    );

  const createAt = (name: string, secretPath: string, value: Uint8Array) =>
    crypto.uuidV4().pipe(
      Effect.mapError(
        (cause) =>
          new SecretStoreTemporaryPathError({
            resource: `secret ${name}`,
            cause,
          }),
      ),
      Effect.flatMap((uuid) => {
        const tempPath = `${secretPath}.${uuid}.tmp`;
        return Effect.gen(function* () {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const file = yield* fileSystem.open(tempPath, { flag: "wx", mode: 0o600 });
              yield* file.writeAll(value);
              yield* file.sync;
            }),
          );
          yield* fileSystem.chmod(tempPath, 0o600);
          yield* fileSystem.link(tempPath, secretPath);
        }).pipe(
          Effect.ensuring(fileSystem.remove(tempPath).pipe(Effect.ignore)),
          Effect.mapError(
            (cause) =>
              new SecretStorePersistError({
                resource: `secret ${name}`,
                cause,
              }),
          ),
        );
      }),
    );

  const getOrCreateRandom: ServerSecretStore["Service"]["getOrCreateRandom"] = (name, bytes) =>
    get(name).pipe(
      // Older builds could persist a 0-byte secret, which is never a usable key. Re-read under
      // the permit, and keep it until an interrupted removal settles, so a caller that saw the
      // empty file cannot remove a key another caller created.
      Effect.filterOrElse(isMissingOrNonEmpty, () =>
        emptySecretRepair.withPermits(1)(
          get(name).pipe(
            Effect.filterOrElse(isMissingOrNonEmpty, () =>
              remove(name).pipe(Effect.as(Option.none<Uint8Array>())),
            ),
            Effect.uninterruptible,
          ),
        ),
      ),
      Effect.flatMap(
        Option.match({
          onSome: Effect.succeed,
          onNone: () =>
            crypto.randomBytes(bytes).pipe(
              Effect.mapError(
                (cause) =>
                  new SecretStoreRandomGenerationError({
                    resource: `secret ${name}`,
                    cause,
                  }),
              ),
              Effect.flatMap((generated) =>
                create(name, generated).pipe(
                  Effect.as(Uint8Array.from(generated)),
                  Effect.catchIf(isSecretStoreError, (error) =>
                    isSecretAlreadyExistsError(error)
                      ? get(name).pipe(
                          Effect.flatMap(
                            Option.match({
                              onSome: Effect.succeed,
                              onNone: () =>
                                Effect.fail(
                                  new SecretStoreConcurrentReadError({
                                    resource: `secret ${name}`,
                                  }),
                                ),
                            }),
                          ),
                        )
                      : Effect.fail(error),
                  ),
                ),
              ),
            ),
        }),
      ),
      Effect.withSpan("ServerSecretStore.getOrCreateRandom"),
    );

  const remove: ServerSecretStore["Service"]["remove"] = (name) =>
    resolveSecretPath(name).pipe(
      Effect.flatMap((secretPath) =>
        mutating(
          name,
          fileSystem.remove(secretPath).pipe(
            Effect.catch((cause) =>
              cause.reason._tag === "NotFound"
                ? Effect.void
                : Effect.fail(
                    new SecretStoreRemoveError({
                      resource: `secret ${name}`,
                      cause,
                    }),
                  ),
            ),
          ),
        ),
      ),
      Effect.withSpan("ServerSecretStore.remove"),
    );

  const revision = Effect.fn("ServerSecretStore.revision")(function* (name: string) {
    yield* resolveSecretPath(name);
    const state = generation(name);
    const before = state.value;
    if (state.pending !== 0) return Option.none<string>();
    return yield* Effect.gen(function* () {
      const current = yield* get(name);
      if (Option.isNone(current) || current.value.byteLength === 0) return Option.none<string>();
      const header = new TextEncoder().encode(
        `${name.length}:${name}:${before}:${current.value.byteLength}:`,
      );
      const payload = new Uint8Array(header.byteLength + current.value.byteLength);
      payload.set(header);
      payload.set(current.value, header.byteLength);
      const tag = yield* mac
        .sign(revisionKey, payload)
        .pipe(
          Effect.mapError(
            (cause) => new SecretStoreReadError({ resource: "credential revision", cause }),
          ),
        );
      if (state.pending !== 0 || state.value !== before) return Option.none<string>();
      return Option.some(Hex.encode(tag));
    }).pipe(
      Effect.onError(() =>
        Effect.sync(() => {
          state.value += 1n;
        }),
      ),
    );
  });

  return ServerSecretStore.of({
    directory: serverConfig.secretsDir,
    revision,
    get,
    set,
    create,
    getOrCreateRandom,
    remove,
  });
});

export const layer = Layer.effect(ServerSecretStore, make);
