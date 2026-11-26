import {
  CodingNativeTranscriptInput,
  CodingNativeTranscriptUnavailable,
  ThreadId,
} from "@t3tools/contracts";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Reservation from "./CodingThreadReservation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as Projects from "./ProjectStore.ts";
import { commandId, revision } from "./CodingNativeTranscriptPlan.ts";

const decode = Schema.decodeEffect(CodingNativeTranscriptInput);
export const make = Effect.gen(function* () {
  const managed = yield* Config.Boolean("T3CODE_ICORE_PROJECTS").pipe(
    Config.withDefault(false),
    Effect.orDie,
  );
  const reservations = yield* Reservation.make;
  const threads = yield* Orchestrator.OrchestratorV2;
  const projects = yield* Projects.ProjectStoreV2;
  const record = Effect.fn("CodingNativeTranscript.record")(
    function* (request: CodingNativeTranscriptInput) {
      if (!managed) return yield* new CodingNativeTranscriptUnavailable();
      const input = yield* decode(request, { onExcessProperty: "error" });
      const threadId = ThreadId.make("icore-" + input.reservation.claim);
      // Recording can only use a reservation that already exists.
      if ((yield* threads.getThreadShell(threadId)) === null)
        return yield* new CodingNativeTranscriptUnavailable();
      const reservation = yield* reservations.reserve(input.reservation);
      const result = yield* threads.dispatch({
        type: "thread.coding-native.record",
        threadId,
        commandId: commandId(input),
        reservationCreatedAt: reservation.created_at,
        recording: input,
      });
      const event = result.storedEvents.at(-1);
      const checkpoint =
        event?.event.type === "thread.metadata-updated"
          ? event.event.payload.codingNative
          : undefined;
      if (
        event?.commandId !== commandId(input) ||
        event?.event.threadId !== threadId ||
        checkpoint?.sequence !== input.sequence ||
        checkpoint.revision !== revision(input) ||
        checkpoint.claim !== input.reservation.claim ||
        checkpoint.intentDigest !== input.reservation.intentDigest ||
        checkpoint.nativeThreadId !== input.nativeThreadId ||
        checkpoint.nativeTurnId !== input.nativeTurnId ||
        checkpoint.outcome !== input.outcome
      )
        return yield* new CodingNativeTranscriptUnavailable();
      const project = yield* projects.get(input.reservation.projectId, { includeDeleted: true });
      const current = yield* threads.getThreadRecords(threadId, []);
      if (
        Option.isNone(project) ||
        project.value.deletedAt !== null ||
        project.value.archivedAt !== null ||
        current.thread.deletedAt !== null ||
        current.thread.archivedAt !== null ||
        current.thread.projectId !== input.reservation.projectId ||
        current.thread.codingNative?.claim !== checkpoint.claim ||
        current.thread.codingNative.sequence < checkpoint.sequence
      )
        return yield* new CodingNativeTranscriptUnavailable();
      return {
        thread_id: threadId,
        sequence: checkpoint.sequence,
        revision: checkpoint.revision,
        outcome: checkpoint.outcome,
      };
    },
    (effect) => effect.pipe(Effect.mapError(() => new CodingNativeTranscriptUnavailable())),
  );
  return { record };
});
