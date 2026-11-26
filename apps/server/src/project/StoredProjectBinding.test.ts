import { assert, it } from "@effect/vitest";
import { EventId, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as Binding from "./StoredProjectBinding.ts";

const at = "2026-11-25T00:00:00.000Z";
const id = ProjectId.make("synthetic-mapped-project");
const created = {
  sequence: 1,
  eventId: EventId.make("synthetic-created"),
  aggregateKind: "project" as const,
  aggregateId: id,
  occurredAt: at,
  commandId: null,
  causationEventId: null,
  correlationId: null,
  metadata: {},
  type: "project.created" as const,
  payload: {
    projectId: id,
    title: "Synthetic",
    workspaceRoot: "/nonexistent/coordination",
    defaultModelSelection: null,
    scripts: [],
    trackedFolders: [{ path: "/nonexistent/repository", name: "Extra" }],
    createdAt: at,
    updatedAt: at,
  },
};

it.layer(ProjectStore.layer.pipe(Layer.provideMerge(SqlitePersistence.layerMemory)))(
  "StoredProjectBinding",
  (it) => {
    it.effect("reads only the requested stored fields without hydration or writes", () =>
      Effect.gen(function* () {
        const store = yield* ProjectStore.ProjectStoreV2;
        const sql = yield* SqlClient.SqlClient;
        yield* store.apply(created);
        const before = yield* sql`SELECT total_changes() AS changes`;
        assert.deepEqual(yield* Binding.read({ projectId: id }), {
          project_id: id,
          workspace_root: "/nonexistent/coordination",
          folders: ["/nonexistent/repository"],
          created_at: at,
          deleted_at: null,
        });
        assert.deepEqual(yield* sql`SELECT total_changes() AS changes`, before);
      }),
    );
    it.effect(
      "returns absence for an unrelated identity instead of searching by name or root",
      () =>
        Effect.gen(function* () {
          const store = yield* ProjectStore.ProjectStoreV2;
          yield* store.apply(created);
          assert.isNull(yield* Binding.read({ projectId: ProjectId.make("other-project") }));
        }),
    );
    it.effect("retains deletion identity for reconciliation rather than hiding a tombstone", () =>
      Effect.gen(function* () {
        const store = yield* ProjectStore.ProjectStoreV2;
        yield* store.apply(created);
        yield* store.apply({
          ...created,
          sequence: 2,
          eventId: EventId.make("synthetic-deleted"),
          type: "project.deleted",
          payload: { projectId: id, deletedAt: at },
        });
        assert.equal((yield* Binding.read({ projectId: id }))?.deleted_at, at);
      }),
    );
  },
);

it.effect("sanitizes storage failures", () =>
  Effect.gen(function* () {
    const error = yield* Binding.read({ projectId: id }).pipe(
      Effect.flip,
      Effect.provide(
        Layer.mock(ProjectStore.ProjectStoreV2)({
          get: () =>
            Effect.fail(
              new ProjectStore.ProjectStoreV2Error({
                operation: "synthetic",
                cause: "private detail",
              }),
            ),
        }),
      ),
    );
    assert.equal(error._tag, "StoredProjectBindingUnavailable");
    assert.notInclude(error.message, "private detail");
  }),
);
