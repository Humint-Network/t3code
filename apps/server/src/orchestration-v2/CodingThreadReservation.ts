// @effect-diagnostics-next-line nodeBuiltinImport:off -- deterministic SHA-256 of validated bounded metadata.
import * as NodeCrypto from "node:crypto";
import {
  CodingThreadReservationInput,
  CodingThreadReservationUnavailable,
  CommandId,
  ThreadId,
} from "@t3tools/contracts";
import * as Config from "effect/Config";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectStore from "./ProjectStore.ts";

const decodeInput = Schema.decodeEffect(CodingThreadReservationInput);

function fingerprint(input: CodingThreadReservationInput): string {
  return NodeCrypto.createHash("sha256")
    .update(
      JSON.stringify([
        "icore-coding-thread-v1",
        input.claim,
        input.intentDigest,
        input.projectId,
        input.title,
        input.instanceId,
        input.model,
        input.runtimeMode,
      ]),
    )
    .digest("hex");
}

// Construct once per authenticated connection, with the immutable supervisor mode.
export const make = Effect.gen(function* () {
  const managed = yield* Config.Boolean("T3CODE_ICORE_PROJECTS").pipe(
    Config.withDefault(false),
    Effect.orDie,
  );
  const projects = yield* ProjectStore.ProjectStoreV2;
  const threads = yield* Orchestrator.OrchestratorV2;
  const reserve = Effect.fn("CodingThreadReservation.reserve")(
    function* (request: CodingThreadReservationInput) {
      if (!managed) return yield* new CodingThreadReservationUnavailable();
      const input = yield* decodeInput(request, {
        onExcessProperty: "error",
      });
      const assertProject = Effect.gen(function* () {
        const project = yield* projects.get(input.projectId, { includeDeleted: true });
        if (
          Option.isNone(project) ||
          project.value.deletedAt !== null ||
          project.value.archivedAt !== null
        )
          return yield* new CodingThreadReservationUnavailable();
      });
      yield* assertProject;
      const threadId = ThreadId.make("icore-" + input.claim);
      // Same claim always targets the same thread. Any different input gets a distinct
      // command, whose attempt to recreate that reserved thread is durably rejected.
      const digest = fingerprint(input);
      const commandId = CommandId.make("icore-reserve-" + digest);
      const result = yield* threads.dispatch({
        type: "thread.create",
        commandId,
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
      const events = result.storedEvents;
      const event = events[0]?.event;
      if (
        events.length !== 1 ||
        event?.type !== "thread.created" ||
        events[0]?.commandId !== commandId ||
        event.threadId !== threadId ||
        event.payload.id !== threadId ||
        event.payload.projectId !== input.projectId ||
        event.payload.title !== input.title ||
        event.payload.modelSelection.instanceId !== input.instanceId ||
        event.payload.modelSelection.model !== input.model ||
        event.payload.runtimeMode !== input.runtimeMode ||
        event.payload.activeProviderThreadId !== null ||
        event.payload.forkedFrom !== null ||
        event.payload.branch !== null ||
        event.payload.worktreePath !== null ||
        event.payload.createdBy !== "user" ||
        event.payload.creationSource !== "server"
      )
        return yield* new CodingThreadReservationUnavailable();
      yield* assertProject;
      const current = yield* threads.getThreadShell(threadId);
      if (
        current === null ||
        current.projectId !== input.projectId ||
        current.deletedAt !== null ||
        current.archivedAt !== null ||
        DateTime.toEpochMillis(current.createdAt) !==
          DateTime.toEpochMillis(event.payload.createdAt)
      )
        return yield* new CodingThreadReservationUnavailable();
      return {
        claim: input.claim,
        intent_digest: input.intentDigest,
        project_id: input.projectId,
        thread_id: threadId,
        created_at: DateTime.formatIso(event.payload.createdAt),
      };
    },
    (effect) => effect.pipe(Effect.mapError(() => new CodingThreadReservationUnavailable())),
  );
  return { reserve };
});
