import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProviderLaunchBindingUnavailable,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import type { ProviderAuthController } from "@t3tools/provider-core/server/auth";
import type { ProviderDriver, ProviderInstance } from "@t3tools/provider-core/server/driver";
import { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";
import * as Effect from "effect/Effect";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as ServerConfig from "../config.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";
import * as Registry from "./ProviderInstanceRegistry.ts";
import * as Credential from "./ProviderCredentialStore.ts";
import * as Binding from "./ProviderLaunchBinding.ts";
import { deriveProviderInstanceConfigMap } from "./ProviderInstanceRegistryHydration.ts";

const projectId = ProjectId.make("icore-" + "b".repeat(48));
const instanceId = ProviderInstanceId.make("binding-endpoint");
const driverKind = ProviderDriverKind.make("codex");
const now = "2026-11-25T00:00:00.000Z";
const input = {
  projectId,
  instanceId,
  model: "model-a",
  runtimeMode: "approval-required" as const,
};
const config = () =>
  Layer.mergeAll(
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-binding-test-" }),
    Settings.ServerSettingsService.layerTest({
      providerInstances: {
        [instanceId]: { driver: driverKind, enabled: true, config: { binary: "synthetic" } },
      },
    }),
  );
const unexpected = () => Effect.die("Preparation must not execute provider operations.");

const fixture = Effect.gen(function* () {
  const settings = yield* Settings.ServerSettingsService;
  const secrets = yield* ServerSecretStore.make;
  const credential = yield* Credential.make(driverKind, instanceId).pipe(
    Effect.provideService(ServerSecretStore.ServerSecretStore, secrets),
  );
  yield* credential.set(new TextEncoder().encode("synthetic-only-credential"));
  let project: ProjectStore.ProjectRow | undefined = {
    projectId,
    hasCustomTitle: false,
    defaultThreadEnvMode: null,
    autoPull: false,
    faviconPath: null,
    projectIcon: null,
    trackedFolders: [],
    pinnedAt: null,
    archivedAt: null,
    title: "Synthetic project",
    workspaceRoot: "/synthetic/project",
    defaultModelSelection: null,
    scripts: [],
    createdAt: now,
    updatedAt: now,
    deletedAt: null,
  };
  let snapshot: ServerProvider = {
    instanceId,
    driver: driverKind,
    enabled: true,
    installed: true,
    status: "ready",
    version: "1",
    auth: { status: "authenticated", email: "synthetic@example.test" },
    checkedAt: now,
    models: [{ slug: "model-a", name: "Model A", isCustom: false, capabilities: null }],
    slashCommands: [],
    skills: [],
    supportedRuntimeModes: ["approval-required", "full-access"],
  };
  let changing = false;
  let awaiting = false;
  let snapshotReads = 0;
  let onSnapshot: (count: number) => Effect.Effect<void> = () => Effect.void;
  let auth: ProviderAuthController | undefined = {
    credentialBinding: credential.binding,
    isChangingCredentials: Effect.sync(() => changing),
    start: unexpected,
    complete: unexpected,
    cancel: unexpected,
    logout: unexpected,
    subscribe: () => Stream.empty,
  };
  const adapter = yield* ProviderAdapterV2.pipe(
    Effect.provide(Layer.mock(ProviderAdapterV2)({ instanceId, driver: driverKind })),
  );
  const driver: ProviderDriver<unknown> = {
    driverKind,
    metadata: { displayName: "Synthetic" },
    configSchema: Schema.Unknown,
    defaultConfig: () => ({}),
    create: (options) =>
      Effect.sync(
        () =>
          ({
            instanceId: options.instanceId,
            driverKind,
            enabled: options.enabled,
            displayName: undefined,
            continuationIdentity: { driverKind, continuationKey: instanceId },
            ...(auth ? { auth } : {}),
            snapshot: {
              getSnapshot: Effect.suspend(() => onSnapshot(++snapshotReads)).pipe(
                Effect.map(() => snapshot),
              ),
              isAwaitingFirstCheck: Effect.sync(() => awaiting),
              refresh: unexpected(),
              streamChanges: Stream.empty,
              resolveMaintenance: unexpected,
              applyUsageLimits: unexpected,
            },
            snapshotForCwd: unexpected,
            orchestrationAdapter: adapter,
            textGeneration: {
              generateTranscriptionPostProcessing: unexpected,
              generateCommitMessage: unexpected,
              generatePrContent: unexpected,
              generateBranchName: unexpected,
              generateThreadTitle: unexpected,
              generateReviewGuide: unexpected,
            },
          }) satisfies ProviderInstance,
      ),
  };
  const { registry, mutator } = yield* Registry.makeProviderInstanceRegistry({
    drivers: [driver],
    configMap: deriveProviderInstanceConfigMap(yield* settings.getSettings),
  }).pipe(Effect.provide(Layer.mock(SecretResolver.SecretResolver)({})));
  const make = (
    selectedRegistry = registry,
    selectedSecrets = secrets,
    selectedProjects?: ProjectStore.ProjectStoreV2["Service"],
  ) =>
    Binding.make.pipe(
      Effect.provideService(Registry.ProviderInstanceRegistry, selectedRegistry),
      Effect.provideService(ServerSecretStore.ServerSecretStore, selectedSecrets),
      Effect.provide(
        selectedProjects
          ? Layer.succeed(ProjectStore.ProjectStoreV2, selectedProjects)
          : Layer.mock(ProjectStore.ProjectStoreV2)({
              get: (id) =>
                Effect.sync(() =>
                  id === projectId ? Option.fromUndefinedOr(project) : Option.none(),
                ),
            }),
      ),
    );
  const binding = yield* make();
  return {
    binding,
    make,
    secrets,
    credential,
    settings,
    registry,
    mutator,
    get snapshot() {
      return snapshot;
    },
    setSnapshot: (patch: Partial<ServerProvider>) => {
      snapshot = { ...snapshot, ...patch };
    },
    get project() {
      return project;
    },
    setProject: (next: ProjectStore.ProjectRow | undefined) => {
      project = next;
    },
    setChanging: (next: boolean) => {
      changing = next;
    },
    setAwaiting: (next: boolean) => {
      awaiting = next;
    },
    setAuth: (next: ProviderAuthController | undefined) => {
      auth = next;
    },
    get auth() {
      return auth;
    },
    onSnapshot: (fn: typeof onSnapshot) => {
      snapshotReads = 0;
      onSnapshot = fn;
    },
    reconcile: Effect.suspend(() => settings.getSettings).pipe(
      Effect.flatMap((current) => mutator.reconcile(deriveProviderInstanceConfigMap(current))),
    ),
  };
});

const layers = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
const unavailable = (effect: ReturnType<Binding.ProviderLaunchBinding["Service"]["read"]>) =>
  Effect.gen(function* () {
    const error = yield* Effect.flip(effect);
    assert.instanceOf(error, ProviderLaunchBindingUnavailable);
    assert.equal(
      error.message,
      "The selected endpoint changed or is not ready. Review its connection and choices again.",
    );
  });

it.layer(layers)("launch bindings", (it) => {
  it.effect("reads real stored projects without hydration or database writes", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const projects = yield* ProjectStore.ProjectStoreV2;
      const sql = yield* SqlClient.SqlClient;
      yield* projects.apply({
        sequence: 1,
        eventId: EventId.make("binding-created"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.created",
        payload: {
          projectId,
          title: "Stored project",
          workspaceRoot: "/nonexistent/synthetic-only",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
        },
      });
      const binding = yield* f.make(f.registry, f.secrets, projects);
      const changesBefore = yield* sql<{ count: number }>`SELECT total_changes() AS count`;
      const first = yield* binding.read(input);
      assert.deepEqual(yield* binding.read(input), first);
      assert.deepEqual(
        yield* sql<{ count: number }>`SELECT total_changes() AS count`,
        changesBefore,
      );
      yield* projects.apply({
        sequence: 2,
        eventId: EventId.make("binding-updated"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: now,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.meta-updated",
        payload: { projectId, workspaceRoot: "/nonexistent/changed", updatedAt: now },
      });
      assert.notEqual((yield* binding.read(input)).revision, first.revision);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          config(),
          ProjectStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)),
        ),
      ),
    ),
  );
  it.effect("returns an opaque stable binding without running a provider", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = yield* f.binding.read(input);
      assert.deepEqual(Object.keys(first).sort(), [
        "available",
        "driver",
        "instance",
        "model",
        "revision",
        "runtime_mode",
      ]);
      assert.match(first.revision, /^[a-f0-9]{64}$/);
      assert.deepEqual(yield* f.binding.read(input), first);
      const restarted = yield* f.make();
      assert.notEqual((yield* restarted.read(input)).revision, first.revision);
      assert.notEqual(
        (yield* f.binding.read({ ...input, runtimeMode: "full-access" })).revision,
        first.revision,
      );
    }).pipe(Effect.provide(config())),
  );

  it.effect("changes when credentials are rewritten or the account changes", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = yield* f.binding.read(input);
      yield* f.credential.set(new TextEncoder().encode("synthetic-only-credential"));
      const second = yield* f.binding.read(input);
      assert.notEqual(second.revision, first.revision);
      f.setSnapshot({ auth: { status: "authenticated", email: "other@example.test" } });
      assert.notEqual((yield* f.binding.read(input)).revision, second.revision);
      yield* f.credential.remove;
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("binds the current project and refuses missing or deleted projects", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = yield* f.binding.read(input);
      f.setProject({ ...f.project!, workspaceRoot: "/synthetic/other" });
      assert.notEqual((yield* f.binding.read(input)).revision, first.revision);
      f.setProject({ ...f.project!, deletedAt: now });
      yield* unavailable(f.binding.read(input));
      f.setProject(undefined);
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses settings ahead of the registry and retires rebuilt instances", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = yield* f.binding.read(input);
      yield* f.settings.updateSettings({
        providerInstances: {
          [instanceId]: { driver: driverKind, enabled: true, config: { binary: "changed" } },
        },
      });
      yield* unavailable(f.binding.read(input));
      yield* f.reconcile;
      assert.notEqual((yield* f.binding.read(input)).revision, first.revision);
      yield* f.mutator.reconcile({});
      yield* unavailable(f.binding.read(input));
      yield* f.reconcile;
      assert.notEqual((yield* f.binding.read(input)).revision, first.revision);
    }).pipe(Effect.provide(config())),
  );

  it.effect("keeps labels stable while enforcing project enablement", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = yield* f.binding.read(input);
      yield* f.settings.updateSettings({
        providerInstances: {
          [instanceId]: {
            driver: driverKind,
            enabled: true,
            config: { binary: "synthetic" },
            displayName: "New label",
          },
        },
      });
      yield* f.reconcile;
      assert.equal((yield* f.binding.read(input)).revision, first.revision);
      yield* f.settings.updateSettings({
        projectSettingsOverrides: {
          [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
        },
      });
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses ambiguous models, retained catalogs and unsupported modes", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* unavailable(f.binding.read({ ...input, model: "absent" }));
      f.setSnapshot({ models: [...f.snapshot.models, ...f.snapshot.models] });
      yield* unavailable(f.binding.read(input));
      f.setSnapshot({ models: f.snapshot.models.slice(0, 1), modelsRetained: true });
      yield* unavailable(f.binding.read(input));
      f.setSnapshot({ modelsRetained: false, supportedRuntimeModes: [] });
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses unavailable, uninstalled, unauthenticated and unchecked endpoints", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.setSnapshot({ installed: false });
      yield* unavailable(f.binding.read(input));
      f.setSnapshot({ installed: true, auth: { status: "unauthenticated" } });
      yield* unavailable(f.binding.read(input));
      f.setSnapshot({ auth: { status: "authenticated" }, availability: "unavailable" });
      yield* unavailable(f.binding.read(input));
      f.setSnapshot({ availability: "available" });
      f.setAwaiting(true);
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses changing credentials and externally owned credentials", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.setChanging(true);
      yield* unavailable(f.binding.read(input));
      f.setChanging(false);
      f.setAuth({
        ...f.auth!,
        credentialBinding: { owner: "provider", key: f.credential.binding.key },
      });
      yield* f.mutator.reconcile({});
      yield* f.reconcile;
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses a project changing during the read", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.onSnapshot((count) =>
        Effect.sync(() => {
          if (count === 2) f.setProject({ ...f.project!, title: "Changed" });
        }),
      );
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses credentials changing during the read", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.onSnapshot((count) =>
        count === 2 ? f.credential.set(new Uint8Array([9])).pipe(Effect.orDie) : Effect.void,
      );
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses registry replacement during the read", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.onSnapshot((count) => (count === 2 ? f.mutator.reconcile({}) : Effect.void));
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses unsupported registry and credential revision capabilities", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const { getConfiguredInstance: _configured, ...withoutConfigured } = f.registry;
      const oldRegistry = yield* f.make(withoutConfigured);
      yield* unavailable(oldRegistry.read(input));
      const { revision: _revision, ...withoutRevision } = f.secrets;
      const oldStore = yield* f.make(f.registry, withoutRevision);
      yield* unavailable(oldStore.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("binds model capabilities and provider version", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const first = yield* f.binding.read(input);
      f.setSnapshot({ models: [{ ...f.snapshot.models[0]!, isCustom: true }] });
      const second = yield* f.binding.read(input);
      assert.notEqual(second.revision, first.revision);
      f.setSnapshot({ version: "2" });
      assert.notEqual((yield* f.binding.read(input)).revision, second.revision);
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses a snapshot for the wrong endpoint or driver", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.setSnapshot({ instanceId: ProviderInstanceId.make("another") });
      yield* unavailable(f.binding.read(input));
      f.setSnapshot({ instanceId, driver: ProviderDriverKind.make("another") });
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses credential transition and pending check starting during the read", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      f.onSnapshot((count) =>
        Effect.sync(() => {
          if (count === 2) f.setChanging(true);
        }),
      );
      yield* unavailable(f.binding.read(input));
      f.setChanging(false);
      f.onSnapshot((count) =>
        Effect.sync(() => {
          if (count === 2) f.setAwaiting(true);
        }),
      );
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("refuses absent authentication and malformed credential bindings", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const original = f.auth!;
      f.setAuth(undefined);
      yield* f.mutator.reconcile({});
      yield* f.reconcile;
      yield* unavailable(f.binding.read(input));
      f.setAuth({ ...original, credentialBinding: { owner: "t3", key: "../synthetic" } });
      yield* f.mutator.reconcile({});
      yield* f.reconcile;
      yield* unavailable(f.binding.read(input));
    }).pipe(Effect.provide(config())),
  );

  it.effect("bounds a stalled read and releases the settings lock", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const entered = yield* Deferred.make<void>();
      f.onSnapshot(() => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)));
      const reading = yield* f.binding.read(input).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* TestClock.adjust("8 seconds");
      yield* unavailable(Fiber.join(reading));
      yield* f.settings.updateSettings({ projectSettingsOverrides: {} });
      f.onSnapshot(() => Effect.void);
      assert.isTrue((yield* f.binding.read(input)).available);
    }).pipe(Effect.provide(config())),
  );
});

const managedAccess = ConfigProvider.layer(
  ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: true }),
);
const accessValue = {
  accessToken: "synthetic-only",
  expiresAt: Number.MAX_SAFE_INTEGER,
  kind: "chatgpt-managed" as const,
  resource: "https://api.openai.com/v1" as const,
};
it.layer(layers)("selected model access", (it) => {
  it.effect("returns only the exact current managed account and never acquires during reads", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let calls = 0;
      Object.assign(f.auth!, {
        modelAccess: Effect.sync(() => {
          calls++;
          return accessValue;
        }),
      });
      const binding = yield* f.binding.read(input);
      assert.equal(calls, 0);
      const result = yield* f.binding.acquire({ ...input, revision: binding.revision });
      assert.deepEqual(result, { binding, ...accessValue });
      assert.equal(calls, 1);
      yield* unavailable(
        f.binding
          .acquire({ ...input, revision: "a".repeat(64) })
          .pipe(Effect.map((v) => v.binding)),
      );
      assert.equal(calls, 1);
    }).pipe(Effect.provide(Layer.merge(config(), managedAccess))),
  );
  it.effect("refuses account rotation during access without exposing a token", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      Object.assign(f.auth!, {
        modelAccess: f.credential
          .set(new TextEncoder().encode("rotated"))
          .pipe(Effect.as(accessValue)),
      });
      const binding = yield* f.binding.read(input);
      yield* unavailable(
        f.binding
          .acquire({ ...input, revision: binding.revision })
          .pipe(Effect.map((v) => v.binding)),
      );
    }).pipe(Effect.provide(Layer.merge(config(), managedAccess))),
  );
  it.effect("refuses absent capability, expired access and invalid resource or token", () =>
    Effect.gen(function* () {
      yield* TestClock.adjust("1 second");
      const f = yield* fixture;
      const binding = yield* f.binding.read(input);
      yield* unavailable(
        f.binding
          .acquire({ ...input, revision: binding.revision })
          .pipe(Effect.map((v) => v.binding)),
      );
      for (const value of [
        { ...accessValue, expiresAt: 1 },
        { ...accessValue, accessToken: "bad\nheader" },
      ]) {
        Object.assign(f.auth!, { modelAccess: Effect.succeed(value) });
        yield* unavailable(
          f.binding
            .acquire({ ...input, revision: binding.revision })
            .pipe(Effect.map((v) => v.binding)),
        );
      }
    }).pipe(Effect.provide(Layer.merge(config(), managedAccess))),
  );
  it.effect("refuses outside immutable iCore-managed mode before credential access", () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      let calls = 0;
      Object.assign(f.auth!, {
        modelAccess: Effect.sync(() => {
          calls++;
          return accessValue;
        }),
      });
      const binding = yield* f.binding.read(input);
      yield* unavailable(
        f.binding
          .acquire({ ...input, revision: binding.revision })
          .pipe(Effect.map((v) => v.binding)),
      );
      assert.equal(calls, 0);
    }).pipe(
      Effect.provide(
        Layer.merge(
          config(),
          ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: false })),
        ),
      ),
    ),
  );
});
