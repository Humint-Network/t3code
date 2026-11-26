// @effect-diagnostics-next-line nodeBuiltinImport:off -- retained test-only database directory.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- retained test-only database path.
import * as NodePath from "node:path";
import * as NodeOS from "node:os";
import { vi } from "vite-plus/test";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type CodingThreadReservationInput,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as Projects from "./ProjectStore.ts";
import * as Registry from "./ProviderAdapterRegistry.ts";
import * as Replay from "./testkit/ProviderReplayHarness.ts";
import * as Reservation from "./CodingThreadReservation.ts";
import * as PullRequests from "./ThreadPullRequestService.ts";

const input: CodingThreadReservationInput = {
  claim: "a".repeat(64),
  intentDigest: "b".repeat(64),
  projectId: ProjectId.make("icore-" + "c".repeat(48)),
  title: "QA 新しい work",
  instanceId: ProviderInstanceId.make("unconfigured-fixture"),
  model: "fixture-model",
  runtimeMode: "approval-required",
};
const at = "2026-11-26T00:00:00.000Z";
const setup = (projectId = input.projectId) =>
  Effect.gen(function* () {
    const projects = yield* Projects.ProjectStoreV2;
    yield* projects.apply({
      sequence: 1,
      eventId: EventId.make("project"),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: at,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: "QA",
        workspaceRoot: "/never/read-this-root",
        defaultModelSelection: null,
        scripts: [],
        createdAt: at,
        updatedAt: at,
      },
    });
  });
const layer = (managed = true, database: typeof Sqlite.layerMemory = Sqlite.layerMemory) =>
  Layer.mergeAll(
    ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: managed })),
    database,
    Projects.layer.pipe(Layer.provide(database)),
    Replay.layerWithRegistry({ name: "coding-reservation" }, Registry.layerFromAdapters([]), {
      databaseLayer: database,
      runEffectWorker: false,
    }),
  );
const provide = <A, E, R>(effect: Effect.Effect<A, E, R>, managed = true) =>
  effect.pipe(Effect.provide(layer(managed)));

it.effect(
  "reserves metadata only, with no provider configured, run, message or provider session",
  () =>
    provide(
      Effect.gen(function* () {
        yield* setup();
        const service = yield* Reservation.make;
        const receipt = yield* service.reserve(input);
        assert.deepEqual(Object.keys(receipt).sort(), [
          "claim",
          "created_at",
          "intent_digest",
          "project_id",
          "thread_id",
        ]);
        assert.equal(receipt.thread_id, "icore-" + input.claim);
        const threads = yield* Orchestrator.OrchestratorV2;
        const projection = yield* threads.getThreadRecords(receipt.thread_id, [
          "runs",
          "providerSessions",
          "messages",
        ]);
        assert.deepEqual(projection.runs, []);
        assert.deepEqual(projection.providerSessions, []);
        assert.deepEqual(projection.messages, []);
        assert.equal(projection.thread.worktreePath, null);
        assert.equal(projection.thread.activeProviderThreadId, null);
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          n: number;
        }>`SELECT COUNT(*) AS n FROM orchestration_events WHERE event_type = 'thread.created'`;
        assert.equal(rows[0]?.n, 1);
      }),
    ),
);
it.effect("concurrent retries and a lost response keep exactly one thread and creation event", () =>
  provide(
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Reservation.make;
      const receipts = yield* Effect.all(
        Array.from({ length: 12 }, () => service.reserve(input)),
        { concurrency: "unbounded" },
      );
      receipts.forEach((receipt) => assert.deepEqual(receipt, receipts[0]));
      assert.deepEqual(yield* service.reserve(input), receipts[0]);
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql<{
          n: number;
        }>`SELECT COUNT(*) AS n FROM orchestration_events WHERE event_type = 'thread.created'`)[0]
          ?.n,
        1,
      );
    }),
  ),
);
it.effect("every changed input conflicts for a used claim, with the original receipt intact", () =>
  provide(
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Reservation.make;
      const first = yield* service.reserve(input);
      yield* setup(ProjectId.make("icore-" + "d".repeat(48)));
      for (const change of [
        { intentDigest: "d".repeat(64) },
        { title: "Other title" },
        { instanceId: ProviderInstanceId.make("other") },
        { model: "other-model" },
        { runtimeMode: "full-access" as const },
        { projectId: ProjectId.make("icore-" + "d".repeat(48)) },
      ])
        assert.equal(
          (yield* service.reserve({ ...input, ...change }).pipe(Effect.flip))._tag,
          "CodingThreadReservationUnavailable",
        );
      assert.deepEqual(yield* service.reserve(input), first);
      const threads = yield* Orchestrator.OrchestratorV2;
      assert.equal((yield* threads.listActiveThreads(input.projectId)).length, 1);
    }),
  ),
);
it.effect(
  "rejects malformed input and all undeclared execution or import fields before dispatch",
  () =>
    provide(
      Effect.gen(function* () {
        yield* setup();
        const threads = yield* Orchestrator.OrchestratorV2;
        using spy = vi.spyOn(threads, "dispatch");
        const service = yield* Reservation.make;
        for (const change of [
          { claim: "not-a-claim" },
          { intentDigest: "" },
          { projectId: "ordinary" },
          { title: "" },
          { title: "x".repeat(121) },
          { model: "x".repeat(201) },
          { runtimeMode: "anything" },
          { initialMessage: { text: "never send" } },
          { importedNativeThread: { nativeId: "owner" } },
          { threadId: "existing-owner-session" },
          { worktreePath: "/owner" },
          { command: "execute" },
        ])
          assert.equal(
            (yield* service
              .reserve({ ...input, ...change } as CodingThreadReservationInput)
              .pipe(Effect.flip))._tag,
            "CodingThreadReservationUnavailable",
          );
        assert.equal(spy.mock.calls.length, 0);
      }),
    ),
);
it.effect("rejects absent, archived and deleted projects before creating a conversation", () =>
  provide(
    Effect.gen(function* () {
      const service = yield* Reservation.make;
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
      yield* setup();
      const sql = yield* SqlClient.SqlClient;
      for (const column of ["archived_at", "deleted_at"]) {
        yield* sql.unsafe(`UPDATE projection_projects SET ${column} = ? WHERE project_id = ?`, [
          at,
          input.projectId,
        ]);
        assert.equal(
          (yield* service.reserve(input).pipe(Effect.flip))._tag,
          "CodingThreadReservationUnavailable",
        );
      }
      assert.equal(
        (yield* sql<{
          n: number;
        }>`SELECT COUNT(*) AS n FROM orchestration_events WHERE event_type = 'thread.created'`)[0]
          ?.n,
        0,
      );
    }),
  ),
);
it.effect("retired conversation IDs cannot be revived by replaying the reservation", () =>
  provide(
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Reservation.make;
      const receipt = yield* service.reserve(input);
      const threads = yield* Orchestrator.OrchestratorV2;
      yield* threads.dispatch({
        type: "thread.archive",
        threadId: receipt.thread_id,
        commandId: CommandId.make("archive"),
      });
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
      yield* threads.dispatch({
        type: "thread.delete",
        threadId: receipt.thread_id,
        commandId: CommandId.make("delete"),
      });
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
    }),
  ),
);
it.effect("does not trust a bare accepted receipt or a mismatched creation event", () =>
  provide(
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Reservation.make;
      const threads = yield* Orchestrator.OrchestratorV2;
      const dispatch = threads.dispatch;
      using spy = vi.spyOn(threads, "dispatch");
      spy.mockImplementation((command) =>
        dispatch(command).pipe(Effect.map((value) => ({ ...value, storedEvents: [] }))),
      );
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
      spy.mockImplementation((command) =>
        dispatch(command).pipe(
          Effect.map((value) => ({
            ...value,
            storedEvents: value.storedEvents.map((stored) => ({
              ...stored,
              commandId: CommandId.make("wrong"),
            })),
          })),
        ),
      );
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
      spy.mockImplementation(dispatch);
      assert.equal((yield* service.reserve(input)).thread_id, "icore-" + input.claim);
    }),
  ),
);
it.effect("refuses a project withdrawn while committing, with no second creation", () =>
  provide(
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Reservation.make;
      const threads = yield* Orchestrator.OrchestratorV2;
      const sql = yield* SqlClient.SqlClient;
      const dispatch = threads.dispatch;
      using spy = vi
        .spyOn(threads, "dispatch")
        .mockImplementation((command) =>
          dispatch(command).pipe(
            Effect.tap(() =>
              sql`UPDATE projection_projects SET deleted_at = ${at} WHERE project_id = ${input.projectId}`.pipe(
                Effect.orDie,
              ),
            ),
          ),
        );
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
      assert.equal(spy.mock.calls.length, 1);
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
      assert.equal(spy.mock.calls.length, 1);
    }),
  ),
);
it.effect(
  "cannot reserve outside managed mode or enable it by changing the caller configuration",
  () =>
    provide(
      Effect.gen(function* () {
        yield* setup();
        const service = yield* Reservation.make;
        const altered = service
          .reserve(input)
          .pipe(
            Effect.provide(
              ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: true })),
            ),
          );
        assert.equal((yield* altered.pipe(Effect.flip))._tag, "CodingThreadReservationUnavailable");
      }),
      false,
    ),
);
it.effect(
  "managed reservation mode never starts the pull-request observer or scans roots",
  () =>
    // No scanner dependencies are provided: even constructing them would be a defect.
    // @effect-diagnostics-next-line unsafeEffectTypeAssertion:off -- verifies this mode needs none of the scanner services.
    Effect.gen(function* () {
      const service = yield* PullRequests.make;
      yield* service.start();
      yield* service.drain;
    }).pipe(
      Effect.provide(
        ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: true })),
      ),
    ) as Effect.Effect<void>,
);
it.effect("reopens the durable database and replays the same conversation without a provider", () =>
  Effect.gen(function* () {
    const directory = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "icore-thread-reservation-"),
    );
    const database = Sqlite.layerFromPath(NodePath.join(directory, "state.sqlite")).pipe(
      Layer.provide(NodeServices.layer),
      Layer.orDie,
    );
    const run = (initialize: boolean) =>
      Effect.scoped(
        Effect.gen(function* () {
          if (initialize) yield* setup();
          const service = yield* Reservation.make;
          return yield* service.reserve(input);
        }).pipe(Effect.provide(layer(true, database))),
      );
    assert.equal((yield* run(false).pipe(Effect.flip))._tag, "CodingThreadReservationUnavailable");
    const first = yield* run(true);
    assert.deepEqual(yield* run(false), first);
  }),
);

it.effect(
  "distinct claims create distinct conversations even with identical names and choices",
  () =>
    provide(
      Effect.gen(function* () {
        yield* setup();
        const service = yield* Reservation.make;
        const first = yield* service.reserve(input);
        const second = yield* service.reserve({ ...input, claim: "f".repeat(64) });
        assert.notEqual(first.thread_id, second.thread_id);
        const threads = yield* Orchestrator.OrchestratorV2;
        assert.equal((yield* threads.listActiveThreads(input.projectId)).length, 2);
      }),
    ),
);
it.effect("never adopts an existing conversation created by another command", () =>
  provide(
    Effect.gen(function* () {
      yield* setup();
      const threads = yield* Orchestrator.OrchestratorV2;
      yield* threads.dispatch({
        type: "thread.create",
        commandId: CommandId.make("existing"),
        threadId: ThreadId.make("icore-" + input.claim),
        projectId: input.projectId,
        title: input.title,
        modelSelection: { instanceId: input.instanceId, model: input.model },
        runtimeMode: input.runtimeMode,
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdBy: "user",
        creationSource: "web",
      });
      const service = yield* Reservation.make;
      assert.equal(
        (yield* service.reserve(input).pipe(Effect.flip))._tag,
        "CodingThreadReservationUnavailable",
      );
      assert.equal((yield* threads.listActiveThreads(input.projectId)).length, 1);
    }),
  ),
);
