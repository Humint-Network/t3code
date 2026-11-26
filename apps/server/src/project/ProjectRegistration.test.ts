import { assert, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, EventId, ProjectId, type ProjectRegistrationInput } from "@t3tools/contracts";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ServerConfig from "../config.ts";
import * as RuntimeLayer from "../orchestration-v2/runtimeLayer.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as Enrichment from "./ProjectEnrichmentService.ts";
import * as ProjectService from "./ProjectService.ts";
import * as Registration from "./ProjectRegistration.ts";
import * as Binding from "./StoredProjectBinding.ts";

const input: ProjectRegistrationInput = {
  projectId: ProjectId.make("icore-" + "a".repeat(48)),
  title: "Shared repository project",
  workspaceRoot: "/synthetic/external/main",
  folders: ["/synthetic/external/extra"],
};
const sideEffects: string[] = [];
const unexpected = (name: string) =>
  Effect.sync(() => {
    sideEffects.push(name);
    throw new Error("Unexpected project filesystem operation: " + name);
  });
const layerFs = Layer.effect(
  FileSystem.FileSystem,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return {
      ...fs,
      stat: (path: string) => (path.startsWith("/synthetic/") ? unexpected("stat") : fs.stat(path)),
      readDirectory: (path: string, options?: Parameters<typeof fs.readDirectory>[1]) =>
        path.startsWith("/synthetic/")
          ? unexpected("readDirectory")
          : fs.readDirectory(path, options),
      exists: (path: string) =>
        path.startsWith("/synthetic/") ? unexpected("exists") : fs.exists(path),
    };
  }),
).pipe(Layer.provide(NodeServices.layer));
const layer = (managed = true) =>
  RuntimeLayer.layerProjectService.pipe(
    Layer.provideMerge(ProjectStore.layer),
    Layer.provideMerge(
      Layer.mock(Enrichment.ProjectEnrichmentService)({
        getAvailable: () => unexpected("getAvailable"),
        peek: () => unexpected("peek"),
        invalidate: () => unexpected("invalidate"),
        readRepositoryIdentity: () => unexpected("readRepositoryIdentity"),
      }),
    ),
    Layer.provideMerge(
      Layer.mock(WorkspacePaths.WorkspacePaths)({
        normalizeWorkspaceRoot: () => unexpected("normalize"),
      }),
    ),
    Layer.provideMerge(SqlitePersistence.layerMemory),
    Layer.provideMerge(ServerSettings.layerTest()),
    Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "project-registration-test-" })),
    Layer.provide(layerFs),
    Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    Layer.provide(
      ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: managed })),
    ),
  );

it.effect("registers external roots and all project read forms without repository access", () =>
  Effect.gen(function* () {
    sideEffects.length = 0;
    const projects = yield* ProjectService.ProjectService;
    const value = yield* Registration.register(input);
    assert.deepEqual(Object.keys(value).sort(), [
      "created_at",
      "deleted_at",
      "folders",
      "project_id",
      "workspace_root",
    ]);
    assert.equal(value.workspace_root, input.workspaceRoot);
    assert.deepEqual(value.folders, input.folders);
    assert.deepEqual(yield* Binding.read({ projectId: input.projectId }), value);
    assert(Option.isSome(yield* projects.getById(input.projectId)));
    assert.equal((yield* projects.snapshot).projects.length, 1);
    assert.equal((yield* projects.listPage({ cursor: 0, limit: 10 })).projects.length, 1);
    assert(Option.isSome(yield* projects.getShell(input.projectId)));
    assert.equal((yield* projects.listShells()).length, 1);
    assert.equal(yield* projects.workspaceScanDirectory(input.workspaceRoot), input.workspaceRoot);
    yield* Effect.yieldNow;
    assert.deepEqual(sideEffects, []);
  }).pipe(Effect.provide(layer())),
);

it.effect(
  "keeps two explicit project identities sharing roots and refuses root-based adoption",
  () =>
    Effect.gen(function* () {
      const projects = yield* ProjectService.ProjectService;
      const first = yield* Registration.register(input);
      const second = yield* Registration.register({
        ...input,
        projectId: ProjectId.make("icore-" + "b".repeat(48)),
      });
      assert.notEqual(first.project_id, second.project_id);
      assert.equal(first.workspace_root, second.workspace_root);
      assert.equal((yield* projects.snapshot).projects.length, 2);
      assert(Option.isNone(yield* projects.getByWorkspaceRoot(input.workspaceRoot)));
      assert(
        Option.isNone(
          yield* projects.getByWorkspaceRoot(input.workspaceRoot, { includeDeleted: true }),
        ),
      );
    }).pipe(Effect.provide(layer())),
);

it.effect("reconciles one accepted registration and rejects changed retry payload", () =>
  Effect.gen(function* () {
    const first = yield* Registration.register(input);
    assert.deepEqual(yield* Registration.register(input), first);
    for (const changed of [
      { title: "Changed" },
      { workspaceRoot: "/synthetic/other" },
      { folders: [] },
    ]) {
      assert.equal(
        (yield* Registration.register({ ...input, ...changed }).pipe(Effect.flip))._tag,
        "ProjectRegistrationUnavailable",
      );
    }
    assert.deepEqual(yield* Binding.read({ projectId: input.projectId }), first);
    const projects = yield* ProjectService.ProjectService;
    assert.equal((yield* projects.snapshot).projects.length, 1);
  }).pipe(Effect.provide(layer())),
);

it.effect("generic mutations and bootstrap cannot edit or adopt managed project records", () =>
  Effect.gen(function* () {
    const value = yield* Registration.register(input);
    const projects = yield* ProjectService.ProjectService;
    const common = { ...input, commandId: CommandId.make("untrusted"), trackedFolders: [] };
    const operations = [
      projects
        .create({ ...common, projectId: ProjectId.make("icore-" + "c".repeat(48)) })
        .pipe(Effect.asVoid),
      projects.bootstrap(common).pipe(Effect.asVoid),
      projects.update({ ...common, title: "Wrong" }).pipe(Effect.asVoid),
      projects
        .delete({ projectId: input.projectId, commandId: common.commandId, force: true })
        .pipe(Effect.asVoid),
    ];
    for (const operation of operations)
      assert.equal((yield* operation.pipe(Effect.flip))._tag, "ProjectInvalidError");
    assert.deepEqual(yield* Binding.read({ projectId: input.projectId }), value);
    assert.equal((yield* projects.snapshot).projects.length, 1);
  }).pipe(Effect.provide(layer())),
);

it.effect("validates exact canonical metadata without turning validation into folder access", () =>
  Effect.gen(function* () {
    sideEffects.length = 0;
    const invalid = [
      { workspaceRoot: "/" },
      { workspaceRoot: "relative" },
      { workspaceRoot: "/synthetic/../other" },
      { workspaceRoot: "/synthetic/line\nnext" },
      { workspaceRoot: "/synthetic//double" },
      { folders: [input.workspaceRoot] },
      { folders: ["/synthetic/same", "/synthetic/same"] },
      { folders: Array.from({ length: 20 }, (_, i) => "/synthetic/" + i) },
      { projectId: ProjectId.make("ordinary-id") },
    ];
    for (const changed of invalid)
      assert.equal(
        (yield* Registration.register({ ...input, ...changed }).pipe(Effect.flip))._tag,
        "ProjectRegistrationUnavailable",
      );
    assert.equal(yield* Binding.read({ projectId: input.projectId }), null);
    assert.deepEqual(sideEffects, []);
  }).pipe(Effect.provide(layer())),
);

it.effect("registration is unavailable outside the supervisor-selected mode", () =>
  Effect.gen(function* () {
    sideEffects.length = 0;
    assert.equal(
      (yield* Registration.register(input).pipe(Effect.flip))._tag,
      "ProjectRegistrationUnavailable",
    );
    assert.equal(yield* Binding.read({ projectId: input.projectId }), null);
    assert.deepEqual(sideEffects, []);
  }).pipe(Effect.provide(layer(false))),
);

it.effect("a deleted identity cannot be revived by replaying registration", () =>
  Effect.gen(function* () {
    yield* Registration.register(input);
    const store = yield* ProjectStore.ProjectStoreV2;
    yield* store.apply({
      sequence: 2,
      eventId: EventId.make("deleted-registration"),
      aggregateKind: "project",
      aggregateId: input.projectId,
      type: "project.deleted",
      occurredAt: "2026-11-25T00:00:00.000Z",
      commandId: CommandId.make("deleted-registration"),
      causationEventId: null,
      correlationId: null,
      metadata: {},
      payload: { projectId: input.projectId, deletedAt: "2026-11-25T00:00:00.000Z" },
    });
    assert.equal(
      (yield* Registration.register(input).pipe(Effect.flip))._tag,
      "ProjectRegistrationUnavailable",
    );
    assert.equal(
      (yield* Binding.read({ projectId: input.projectId }))?.deleted_at,
      "2026-11-25T00:00:00.000Z",
    );
  }).pipe(Effect.provide(layer())),
);

it.effect("the project authority mode is fixed when the service starts", () =>
  Effect.gen(function* () {
    const value = yield* Registration.register(input);
    const reread = yield* Registration.register(input).pipe(
      Effect.provide(
        ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: false })),
      ),
    );
    assert.deepEqual(reread, value);
  }).pipe(Effect.provide(layer())),
);
