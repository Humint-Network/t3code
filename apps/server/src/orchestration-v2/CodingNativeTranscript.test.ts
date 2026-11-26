import {
  AuthEnvironmentMaintainScope,
  AuthOrchestrationReadScope,
  AuthOrchestrationOperateScope,
  WS_METHODS,
  WsRpcGroup,
  CodingNativeTranscriptInput as NativeInput,
} from "@t3tools/contracts";
import * as RpcTest from "effect/rpc/RpcTest";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
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
import * as Native from "./CodingNativeTranscript.ts";
import * as Plan from "./CodingNativeTranscriptPlan.ts";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import { OrchestrationV2Command, type CodingNativeTranscriptInput } from "@t3tools/contracts";

const decodeClientCommand = Schema.decodeUnknownOption(OrchestrationV2Command);
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
  ).pipe(
    Layer.provide(
      ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: managed })),
    ),
  );
const provide = <A, E, R>(effect: Effect.Effect<A, E, R>, managed = true) =>
  effect.pipe(Effect.provide(layer(managed)));

const first: CodingNativeTranscriptInput = {
  reservation: input,
  nativeThreadId: "native-thread",
  nativeTurnId: "native-turn",
  sequence: 0,
  previousRevision: null,
  message: {
    id: "owner",
    role: "user",
    text: "QA exact note שלום 😀",
    phase: null,
    complete: true,
  },
  outcome: "active",
};
const next = (
  prior: CodingNativeTranscriptInput,
  changes: Partial<CodingNativeTranscriptInput> = {},
): CodingNativeTranscriptInput => ({
  ...prior,
  sequence: prior.sequence + 1,
  previousRevision: Plan.revision(prior),
  message: null,
  ...changes,
});
const begin = Effect.gen(function* () {
  yield* setup();
  const reservations = yield* Reservation.make;
  const reserved = yield* reservations.reserve(input);
  const service = yield* Native.make;
  const threads = yield* Orchestrator.OrchestratorV2;
  const read = threads.getThreadRecords(reserved.thread_id, [
    "messages",
    "turnItems",
    "runs",
    "providerSessions",
    "providerThreads",
  ]);
  return { service, threads, reserved, read };
});
const rejected = (operation: ReturnType<Effect.Success<typeof Native.make>["record"]>) =>
  operation.pipe(
    Effect.result,
    Effect.map((result) => {
      assert.equal(result._tag, "Failure", "conflicting observation must be refused");
      if (result._tag === "Failure")
        assert.equal(result.failure._tag, "CodingNativeTranscriptUnavailable");
    }),
  );

it.effect(
  "records public messages and timeline in the reserved conversation without execution",
  () =>
    provide(
      Effect.gen(function* () {
        const { service, read } = yield* begin;
        const receipt = yield* service.record(first);
        const p = yield* read;
        assert.equal(p.messages.length, 1);
        assert.equal(p.messages[0]?.text, first.message?.text);
        assert.equal(p.turnItems.length, 1);
        assert.equal(p.turnItems[0]?.type, "user_message");
        assert.equal(p.thread.codingNative?.revision, receipt.revision);
        assert.equal(p.thread.codingNative?.sequence, 0);
        assert.deepEqual(p.runs, []);
        assert.deepEqual(p.providerSessions, []);
        assert.deepEqual(p.providerThreads, []);
        const sql = yield* SqlClient.SqlClient;
        assert.equal(
          (yield* sql<{ n: number }>`SELECT COUNT(*) AS n FROM orchestration_v2_effect_outbox`)[0]
            ?.n,
          0,
        );
      }),
    ),
);
it.effect("updates one assistant message in first-seen order then seals its completed text", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      yield* service.record(first);
      const partial = next(first, {
        message: {
          id: "answer",
          role: "assistant",
          text: "Partial",
          phase: "commentary",
          complete: false,
        },
      });
      yield* service.record(partial);
      const a = yield* read;
      const done = next(partial, {
        message: {
          ...partial.message!,
          text: "Final reply",
          phase: "final_answer",
          complete: true,
        },
      });
      yield* service.record(done);
      const b = yield* read;
      assert.equal(a.messages.length, 2);
      assert.equal(b.messages.length, 2);
      assert.deepEqual(
        a.messages.map((m) => m.id),
        b.messages.map((m) => m.id),
      );
      assert.equal(b.messages[1]?.text, "Final reply");
      assert.equal(b.messages[1]?.streaming, false);
      assert.deepEqual(
        a.turnItems.map((m) => m.ordinal),
        b.turnItems.map((m) => m.ordinal),
      );
      yield* rejected(
        service.record(next(done, { message: { ...done.message!, text: "Changed" } })),
      );
      yield* rejected(
        service.record(next(done, { message: { ...done.message!, complete: false } })),
      );
      yield* rejected(
        service.record(next(done, { message: { ...done.message!, phase: "commentary" } })),
      );
    }),
  ),
);
it.effect("terminal observations retain partial text and explicitly mark uncertain outcomes", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      yield* service.record(first);
      const partial = next(first, {
        message: {
          id: "answer",
          role: "assistant",
          text: "Unfinished",
          phase: null,
          complete: false,
        },
      });
      yield* service.record(partial);
      const end = next(partial, { outcome: "uncertain" });
      yield* service.record(end);
      const p = yield* read;
      assert.equal(p.thread.codingNative?.outcome, "uncertain");
      assert.equal(p.messages[1]?.text, "Unfinished");
      assert.equal(p.messages[1]?.streaming, false);
      assert.equal(p.turnItems.find((i) => i.type === "assistant_message")?.status, "interrupted");
      const notice = p.turnItems.find((i) => i.type === "system_notice");
      assert.equal(notice?.type, "system_notice");
      if (notice?.type === "system_notice") assert.include(notice.message, "uncertain");
      yield* rejected(service.record(next(end, { outcome: "completed" })));
    }),
  ),
);
it.effect("identical concurrent retries and lost replies append only one message", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      const receipts = yield* Effect.all(
        Array.from({ length: 8 }, () => service.record(first)),
        { concurrency: "unbounded" },
      );
      receipts.forEach((r) => assert.deepEqual(r, receipts[0]));
      assert.equal((yield* read).messages.length, 1);
      const later = next(first, { outcome: "completed" });
      yield* service.record(later);
      assert.deepEqual(yield* service.record(first), receipts[0]);
      assert.equal((yield* read).thread.codingNative?.sequence, 1);
    }),
  ),
);
it.effect("rejects sequence gaps regressions and wrong previous revisions", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      yield* service.record(first);
      for (const change of [
        { sequence: 2 },
        { sequence: 0 },
        { previousRevision: "f".repeat(64) },
        { previousRevision: null },
      ])
        yield* rejected(service.record(next(first, change)));
      assert.equal((yield* read).thread.codingNative?.sequence, 0);
    }),
  ),
);
it.effect("concurrent conflicting observations at one sequence have exactly one winner", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      yield* service.record(first);
      const a = next(first, {
        message: { id: "reply", role: "assistant", text: "A", phase: null, complete: true },
      });
      const b = { ...a, message: { ...a.message!, text: "B" } };
      const results = yield* Effect.all(
        [service.record(a).pipe(Effect.result), service.record(b).pipe(Effect.result)],
        { concurrency: "unbounded" },
      );
      assert.equal(results.filter((r) => r._tag === "Success").length, 1);
      assert.equal((yield* read).messages.length, 2);
    }),
  ),
);
it.effect("changed native identities and later owner messages cannot enter the history", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      yield* service.record(first);
      for (const change of [
        { nativeThreadId: "other" },
        { nativeTurnId: "other" },
        { message: first.message },
      ])
        yield* rejected(service.record(next(first, change)));
      assert.equal((yield* read).messages.length, 1);
    }),
  ),
);
it.effect("first observation requires an exact complete owner-message shape", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      for (const change of [
        { sequence: 1 },
        { previousRevision: "a".repeat(64) },
        { message: null },
        { outcome: "completed" as const },
        { message: { ...first.message!, role: "assistant" as const } },
        { message: { ...first.message!, complete: false } },
        { message: { ...first.message!, phase: "commentary" as const } },
      ])
        yield* rejected(service.record({ ...first, ...change }));
      assert.equal((yield* read).messages.length, 0);
    }),
  ),
);
it.effect("refuses undeclared private fields and malformed bounded observations", () =>
  provide(
    Effect.gen(function* () {
      const { service, read } = yield* begin;
      for (const value of [
        { ...first, secret: "QA_SECRET" },
        { ...first, message: { ...first.message!, tool: "exec" } },
        { ...first, nativeTurnId: "../escape" },
        { ...first, sequence: 4098 },
        { ...first, message: { ...first.message!, text: "😀".repeat(65537) } },
      ])
        yield* rejected(service.record(value as CodingNativeTranscriptInput));
      assert.equal((yield* read).messages.length, 0);
    }),
  ),
);
it.effect("recording never creates an absent reservation or adopts another thread", () =>
  provide(
    Effect.gen(function* () {
      yield* setup();
      const service = yield* Native.make;
      yield* rejected(service.record(first));
      const threads = yield* Orchestrator.OrchestratorV2;
      assert.deepEqual(yield* threads.listActiveThreads(input.projectId), []);
      yield* threads.dispatch({
        type: "thread.create",
        commandId: CommandId.make("foreign"),
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
      yield* rejected(service.record(first));
    }),
  ),
);
it.effect("changed reservation content and archived project or thread refuse recording", () =>
  provide(
    Effect.gen(function* () {
      const { service, read, reserved, threads } = yield* begin;
      yield* rejected(
        service.record({ ...first, reservation: { ...input, intentDigest: "d".repeat(64) } }),
      );
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE projection_projects SET archived_at=${at} WHERE project_id=${input.projectId}`;
      yield* rejected(service.record(first));
      yield* sql`UPDATE projection_projects SET archived_at=NULL WHERE project_id=${input.projectId}`;
      yield* threads.dispatch({
        type: "thread.archive",
        commandId: CommandId.make("archive"),
        threadId: reserved.thread_id,
      });
      yield* rejected(service.record(first));
      assert.equal((yield* read).messages.length, 0);
    }),
  ),
);
it.effect("client commands cannot decode the internal native recording operation", () =>
  provide(
    Effect.gen(function* () {
      const { reserved } = yield* begin;
      const command = {
        type: "thread.coding-native.record",
        commandId: Plan.commandId(first),
        threadId: reserved.thread_id,
        reservationCreatedAt: reserved.created_at,
        recording: first,
      };
      assert.isTrue(Option.isNone(decodeClientCommand(command)));
    }),
  ),
);
it.effect("managed mode is captured and cannot be enabled by a caller", () =>
  provide(
    Effect.gen(function* () {
      const service = yield* Native.make;
      yield* rejected(
        service
          .record(first)
          .pipe(
            Effect.provide(
              ConfigProvider.layer(ConfigProvider.fromUnknown({ T3CODE_ICORE_PROJECTS: true })),
            ),
          ),
      );
    }),
    false,
  ),
);
it.effect("durable reopen preserves canonical messages checkpoint and retry receipts", () =>
  Effect.gen(function* () {
    const directory = NodeFS.mkdtempSync(
      NodePath.join(NodeOS.tmpdir(), "icore-native-transcript-"),
    );
    const database = Sqlite.layerFromPath(NodePath.join(directory, "state.sqlite")).pipe(
      Layer.provide(NodeServices.layer),
      Layer.orDie,
    );
    const run = (initialize: boolean) =>
      Effect.scoped(
        Effect.gen(function* () {
          if (initialize) {
            yield* setup();
            const reservations = yield* Reservation.make;
            yield* reservations.reserve(input);
          }
          const service = yield* Native.make;
          const receipt = yield* service.record(first);
          const threads = yield* Orchestrator.OrchestratorV2;
          const p = yield* threads.getThreadRecords(receipt.thread_id, ["messages", "turnItems"]);
          return {
            receipt,
            messages: p.messages.map((m) => ({ id: m.id, text: m.text })),
            checkpoint: p.thread.codingNative,
            items: p.turnItems.map((i) => i.id),
          };
        }).pipe(Effect.provide(layer(true, database))),
      );
    const before = yield* run(true);
    assert.deepEqual(yield* run(false), before);
  }),
);
it.effect("internal canonical recording dispatch persists an observation", () =>
  provide(
    Effect.gen(function* () {
      const { threads, reserved, read } = yield* begin;
      yield* threads.dispatch({
        type: "thread.coding-native.record",
        commandId: Plan.commandId(first),
        threadId: reserved.thread_id,
        reservationCreatedAt: reserved.created_at,
        recording: first,
      });
      assert.equal((yield* read).messages.length, 1);
    }),
  ),
);

it.effect("the cumulative public history has a byte bound independent of each message", () =>
  provide(
    Effect.gen(function* () {
      const { service, read, reserved } = yield* begin;
      yield* service.record(first);
      const p = yield* read;
      const full = { ...p, messages: [{ ...p.messages[0]!, text: "X".repeat(8 * 1024 * 1024) }] };
      const update = next(first, {
        message: { id: "reply", role: "assistant", text: "Y", phase: null, complete: true },
      });
      const now = yield* DateTime.now;
      assert.throws(() => Plan.plan(full, update, reserved.created_at, now), /conflicts/);
    }),
  ),
);
it.effect("current project withdrawal during recording withholds acknowledgement", () =>
  provide(
    Effect.gen(function* () {
      const { service, threads } = yield* begin;
      const original = threads.dispatch;
      const sql = yield* SqlClient.SqlClient;
      using _spy = vi
        .spyOn(threads, "dispatch")
        .mockImplementation((command) =>
          original(command).pipe(
            Effect.tap(() =>
              command.type === "thread.coding-native.record"
                ? sql`UPDATE projection_projects SET archived_at=${at} WHERE project_id=${input.projectId}`.pipe(
                    Effect.orDie,
                  )
                : Effect.void,
            ),
          ),
        );
      yield* rejected(service.record(first));
    }),
  ),
);
it.effect("an altered stored receipt cannot acknowledge a different observation", () =>
  provide(
    Effect.gen(function* () {
      const { service, threads } = yield* begin;
      const original = threads.dispatch;
      using _spy = vi
        .spyOn(threads, "dispatch")
        .mockImplementation((command) =>
          original(command).pipe(
            Effect.map((result) =>
              command.type === "thread.coding-native.record"
                ? { ...result, storedEvents: [] }
                : result,
            ),
          ),
        );
      yield* rejected(service.record(first));
    }),
  ),
);
it.effect(
  "the internal orchestrator independently refuses native recording outside managed mode",
  () =>
    provide(
      Effect.gen(function* () {
        yield* setup();
        const threads = yield* Orchestrator.OrchestratorV2;
        const threadId = ThreadId.make("icore-" + input.claim);
        const created = yield* threads.dispatch({
          type: "thread.create",
          commandId: CommandId.make("native-unmanaged"),
          threadId,
          projectId: input.projectId,
          title: input.title,
          modelSelection: { instanceId: input.instanceId, model: input.model },
          runtimeMode: input.runtimeMode,
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdBy: "user",
          creationSource: "server",
        });
        const event = created.storedEvents[0]!.event;
        assert.equal(event.type, "thread.created");
        if (event.type !== "thread.created") return;
        const result = yield* threads
          .dispatch({
            type: "thread.coding-native.record",
            commandId: Plan.commandId(first),
            threadId,
            reservationCreatedAt: DateTime.formatIso(event.payload.createdAt),
            recording: first,
          })
          .pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.equal((yield* threads.getThreadRecords(threadId, ["messages"])).messages.length, 0);
      }),
      false,
    ),
);

it.effect("the internal recorder binds command identity and rechecks project availability", () =>
  provide(
    Effect.gen(function* () {
      const { threads, reserved, read } = yield* begin;
      const command = {
        type: "thread.coding-native.record" as const,
        commandId: Plan.commandId(first),
        threadId: reserved.thread_id,
        reservationCreatedAt: reserved.created_at,
        recording: first,
      };
      assert.equal(
        (yield* threads
          .dispatch({ ...command, commandId: CommandId.make("wrong-receipt") })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE projection_projects SET archived_at=${at} WHERE project_id=${input.projectId}`;
      assert.equal((yield* threads.dispatch(command).pipe(Effect.result))._tag, "Failure");
      assert.equal((yield* read).messages.length, 0);
    }),
  ),
);
it.effect("the pure plan refuses changed reservation provenance and foreign provider history", () =>
  provide(
    Effect.gen(function* () {
      const { read, reserved } = yield* begin;
      const p = yield* read;
      const now = yield* DateTime.now;
      for (const thread of [
        { ...p.thread, title: "changed" },
        { ...p.thread, creationSource: "web" as const },
        { ...p.thread, modelSelection: { ...p.thread.modelSelection, model: "other" } },
        { ...p.thread, projectId: ProjectId.make("other") },
      ]) {
        assert.throws(
          () => Plan.plan({ ...p, thread }, first, reserved.created_at, now),
          /conflicts/,
        );
      }
      assert.throws(() => Plan.plan(p, first, "2026-11-24T00:00:00.000Z", now), /conflicts/);
      for (const key of ["runs", "providerThreads", "providerSessions"] as const) {
        assert.throws(
          () => Plan.plan({ ...p, [key]: [{}] }, first, reserved.created_at, now),
          /conflicts/,
        );
      }
    }),
  ),
);

const decodeWireObservation = Schema.decodeUnknownOption(NativeInput);
const recordingRpcGroup = WsRpcGroup.omit(
  ...[...WsRpcGroup.requests.keys()].filter(
    (
      tag,
    ): tag is Exclude<
      keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES,
      typeof WS_METHODS.serverRecordCodingTranscript
    > => tag !== WS_METHODS.serverRecordCodingTranscript,
  ),
);
it.effect(
  "recording RPC requires supervisor maintenance and cannot borrow ordinary work access",
  () =>
    provide(
      Effect.scoped(
        Effect.gen(function* () {
          const { service, read } = yield* begin;
          let calls = 0;
          const handler = recordingRpcGroup.toLayerHandler(
            WS_METHODS.serverRecordCodingTranscript,
            (request) =>
              Effect.sync(() => {
                calls += 1;
              }).pipe(Effect.andThen(service.record(request))),
          );
          for (const scopes of [
            [],
            [AuthOrchestrationReadScope],
            [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
          ]) {
            const client = yield* RpcTest.makeClient(recordingRpcGroup).pipe(
              Effect.provide(Layer.merge(handler, RpcAuthorization.layer(scopes))),
            );
            const result = yield* client[WS_METHODS.serverRecordCodingTranscript](first).pipe(
              Effect.result,
            );
            assert.equal(result._tag, "Failure");
            if (result._tag === "Failure")
              assert.equal(result.failure._tag, "EnvironmentAuthorizationError");
          }
          assert.equal(calls, 0);
          assert.equal((yield* read).messages.length, 0);
          const client = yield* RpcTest.makeClient(recordingRpcGroup).pipe(
            Effect.provide(
              Layer.merge(handler, RpcAuthorization.layer([AuthEnvironmentMaintainScope])),
            ),
          );
          const receipt = yield* client[WS_METHODS.serverRecordCodingTranscript](first);
          assert.equal(receipt.revision, Plan.revision(first));
          assert.equal((yield* read).messages.length, 1);
          assert.deepEqual(yield* client[WS_METHODS.serverRecordCodingTranscript](first), receipt);
          assert.equal((yield* read).messages.length, 1);
        }),
      ),
    ),
);
it("wire schema refuses private excess fields before service dispatch", () => {
  for (const value of [
    { ...first, private: "secret" },
    { ...first, message: { ...first.message, reasoning: "private" } },
    { ...first, reservation: { ...first.reservation, token: "private" } },
  ])
    assert.isTrue(Option.isNone(decodeWireObservation(value)));
  assert.isTrue(Option.isSome(decodeWireObservation(first)));
});

it("revision matches the supervisor transport Unicode fixture", () => {
  const observation: CodingNativeTranscriptInput = {
    reservation: {
      claim: "a".repeat(64),
      intentDigest: "b".repeat(64),
      projectId: ProjectId.make("icore-" + "c".repeat(48)),
      title: "New work",
      instanceId: ProviderInstanceId.make("qa-endpoint"),
      model: "qa-model",
      runtimeMode: "approval-required",
    },
    nativeThreadId: "native-thread",
    nativeTurnId: "native-turn",
    sequence: 0,
    previousRevision: null,
    message: {
      id: "owner",
      role: "user",
      text: "Exact message שלום\n🙂",
      phase: null,
      complete: true,
    },
    outcome: "active",
  };
  assert.equal(
    Plan.revision(observation),
    "9a62526e75544c77d78cf5218cec17ea8c868137fe2d99db1f80a4385d9264e4",
  );
});
