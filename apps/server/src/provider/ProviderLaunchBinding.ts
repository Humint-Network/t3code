import {
  ProviderModelAccessInput,
  ProviderModelAccessResult,
  ProviderLaunchBindingInput,
  ProviderLaunchBindingResult,
  ProviderLaunchBindingUnavailable,
  RuntimeMode,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerProviderModel,
  resolveProjectProviderInstanceEnabled,
  type ServerProvider,
} from "@t3tools/contracts";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import * as Config from "effect/Config";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Mac from "effect/crypto/Mac";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as Settings from "../serverSettings.ts";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import { deriveProviderInstanceConfigMap } from "./ProviderInstanceRegistryHydration.ts";

const decodeInput = Schema.decodeEffect(ProviderLaunchBindingInput);
const decodeAccessInput = Schema.decodeEffect(ProviderModelAccessInput);
const decodeAccessResult = Schema.decodeUnknownEffect(ProviderModelAccessResult);

const Identity = Schema.Struct({
  project: ProjectStore.ProjectRow,
  instance: ProviderInstanceId,
  driver: ProviderDriverKind,
  generation: Schema.String,
  credential: Schema.String,
  model: ServerProviderModel,
  mode: RuntimeMode,
  version: Schema.NullOr(Schema.String),
  installedVersion: Schema.NullOr(Schema.String),
  authType: Schema.NullOr(Schema.String),
  email: Schema.NullOr(Schema.String),
  profile: Schema.NullOr(Schema.String),
  workspace: Schema.NullOr(Schema.String),
  endpoint: Schema.NullOr(Schema.String),
});
const encodeIdentity = Schema.encodeEffect(Schema.fromJsonString(Identity));

export class ProviderLaunchBinding extends Context.Service<
  ProviderLaunchBinding,
  {
    readonly acquire: (
      input: ProviderModelAccessInput,
    ) => Effect.Effect<ProviderModelAccessResult, ProviderLaunchBindingUnavailable>;
    readonly read: (
      input: ProviderLaunchBindingInput,
    ) => Effect.Effect<ProviderLaunchBindingResult, ProviderLaunchBindingUnavailable>;
  }
>()("t3/provider/ProviderLaunchBinding") {}

const selected = (
  snapshot: ServerProvider,
  instance: ProviderInstance,
  input: ProviderLaunchBindingInput,
) => {
  if (
    snapshot.instanceId !== input.instanceId ||
    snapshot.driver !== instance.driverKind ||
    !snapshot.enabled ||
    !snapshot.installed ||
    snapshot.status !== "ready" ||
    snapshot.availability === "unavailable" ||
    snapshot.modelsRetained === true ||
    snapshot.auth.status !== "authenticated" ||
    !snapshot.supportedRuntimeModes?.includes(input.runtimeMode)
  )
    return undefined;
  const models = snapshot.models.filter((model) => model.slug === input.model);
  return models.length === 1 ? models[0] : undefined;
};

export const make = Effect.gen(function* () {
  const managed = yield* Config.Boolean("T3CODE_ICORE_PROJECTS").pipe(
    Config.withDefault(false),
    Effect.orDie,
  );
  const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
  const settings = yield* Settings.ServerSettingsService;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const credentials = yield* ServerSecretStore.ServerSecretStore;
  const mac = yield* Mac.Mac;
  const key = yield* mac.generateKey(Mac.hmacSha256);
  const generations = new WeakMap<ProviderInstance, string>();
  let serial = 0n;

  const resolve = Effect.fn("ProviderLaunchBinding.resolve")(
    function* (raw: ProviderLaunchBindingInput, accessRevision?: string) {
      const input = yield* decodeInput(raw, { onExcessProperty: "error" });
      if (accessRevision !== undefined && !managed)
        return yield* new ProviderLaunchBindingUnavailable();
      const unavailable = new ProviderLaunchBindingUnavailable();
      const configured = registry.getConfiguredInstance;
      const credentialRevision = credentials.revision;
      if (!configured || !credentialRevision) return yield* unavailable;
      return yield* settings.withSettingsSnapshot((current) =>
        Effect.gen(function* () {
          if (!resolveProjectProviderInstanceEnabled(current, input.projectId, input.instanceId))
            return yield* unavailable;
          const expected = deriveProviderInstanceConfigMap(current)[input.instanceId];
          if (!expected) return yield* unavailable;
          const instance = yield* configured(input.instanceId, expected);
          if (!instance?.enabled) return yield* unavailable;
          const auth = instance.auth;
          const credential = auth?.credentialBinding;
          if (
            credential?.owner !== "t3" ||
            !/^provider-auth-[a-f0-9]{64}$/.test(credential.key) ||
            !auth?.isChangingCredentials ||
            (yield* auth.isChangingCredentials) !== false
          )
            return yield* unavailable;
          const credentialKey = credential.key;
          if (
            instance.snapshot.isAwaitingFirstCheck &&
            (yield* instance.snapshot.isAwaitingFirstCheck)
          )
            return yield* unavailable;
          const project = yield* projects.get(input.projectId);
          if (Option.isNone(project) || project.value.deletedAt !== null) return yield* unavailable;
          const revision = yield* credentialRevision(credentialKey);
          if (Option.isNone(revision)) return yield* unavailable;
          let generation = generations.get(instance);
          if (generation === undefined) {
            generation = (++serial).toString();
            generations.set(instance, generation);
          }
          const effectiveGeneration = generation;
          const identity = Effect.fnUntraced(function* () {
            const snapshot = yield* instance.snapshot.getSnapshot;
            const model = selected(snapshot, instance, input);
            if (!model) return yield* unavailable;
            const liveProject = yield* projects.get(input.projectId);
            if (Option.isNone(liveProject) || liveProject.value.deletedAt !== null)
              return yield* unavailable;
            return yield* encodeIdentity({
              project: liveProject.value,
              instance: input.instanceId,
              driver: instance.driverKind,
              generation: effectiveGeneration,
              credential: revision.value,
              model,
              mode: input.runtimeMode,
              version: snapshot.version,
              installedVersion: snapshot.installedVersion ?? null,
              authType: snapshot.auth.type ?? null,
              email: snapshot.auth.email ?? null,
              profile: snapshot.auth.profileId ?? null,
              workspace: snapshot.auth.workspaceId ?? null,
              endpoint: snapshot.auth.endpoint ?? null,
            });
          });
          const before = yield* identity();
          const tag = yield* mac.sign(key, new TextEncoder().encode(before));
          let access:
            | {
                readonly accessToken: string;
                readonly expiresAt: number;
                readonly kind: "chatgpt-managed";
                readonly resource: "https://api.openai.com/v1";
              }
            | undefined;
          if (accessRevision !== undefined) {
            if (
              instance.driverKind !== "codex" ||
              Hex.encode(tag) !== accessRevision ||
              !auth.modelAccess
            )
              return yield* unavailable;
            access = yield* auth.modelAccess;
          }
          const after = yield* identity();
          const latestCredential = yield* credentialRevision(credentialKey);
          if (
            instance.snapshot.isAwaitingFirstCheck &&
            (yield* instance.snapshot.isAwaitingFirstCheck)
          )
            return yield* unavailable;
          if (
            before !== after ||
            Option.isNone(latestCredential) ||
            latestCredential.value !== revision.value ||
            (yield* configured(input.instanceId, expected)) !== instance ||
            instance.auth !== auth ||
            auth.credentialBinding?.owner !== "t3" ||
            auth.credentialBinding?.key !== credentialKey ||
            (yield* auth.isChangingCredentials) !== false
          )
            return yield* unavailable;
          const binding = {
            instance: input.instanceId,
            driver: instance.driverKind,
            model: input.model,
            runtime_mode: input.runtimeMode,
            revision: Hex.encode(tag),
            available: true as const,
          };
          return { binding, access };
        }),
      );
    },
    (effect) =>
      effect.pipe(
        Effect.timeout("8 seconds"),
        Effect.mapError(() => new ProviderLaunchBindingUnavailable()),
      ),
  );
  const read = Effect.fn("ProviderLaunchBinding.read")(function* (
    input: ProviderLaunchBindingInput,
  ) {
    return (yield* resolve(input)).binding;
  });
  const acquire = Effect.fn("ProviderLaunchBinding.acquire")(
    function* (raw: ProviderModelAccessInput) {
      const { revision, ...input } = yield* decodeAccessInput(raw, { onExcessProperty: "error" });
      const result = yield* resolve(input, revision);
      if (!result.access || result.access.expiresAt <= (yield* Clock.currentTimeMillis))
        return yield* new ProviderLaunchBindingUnavailable();
      return yield* decodeAccessResult(
        { binding: result.binding, ...result.access },
        { onExcessProperty: "error" },
      );
    },
    (effect) => effect.pipe(Effect.mapError(() => new ProviderLaunchBindingUnavailable())),
  );
  return ProviderLaunchBinding.of({ read, acquire });
});

export const layer = Layer.effect(ProviderLaunchBinding, make);
