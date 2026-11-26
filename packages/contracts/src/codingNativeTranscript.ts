import * as Schema from "effect/Schema";
import { ThreadId } from "./baseSchemas.ts";
import { CodingThreadReservationInput } from "./codingThreadReservation.ts";

const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const NativeId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,200}$/));
export const CodingNativeOutcome = Schema.Literals([
  "active",
  "completed",
  "failed",
  "interrupted",
  "stopped",
  "uncertain",
  "recovery_required",
]);
const NativeObservation = Schema.Struct({
  reservation: CodingThreadReservationInput,
  nativeThreadId: NativeId,
  nativeTurnId: NativeId,
  sequence: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4097 })),
  previousRevision: Schema.NullOr(Digest),
  message: Schema.NullOr(
    Schema.Struct({
      id: NativeId,
      role: Schema.Literals(["user", "assistant"]),
      text: Schema.String.check(Schema.isMaxLength(262144)),
      phase: Schema.NullOr(Schema.Literals(["commentary", "partial_answer", "final_answer"])),
      complete: Schema.Boolean,
    }),
  ),
  outcome: CodingNativeOutcome,
});
// Check raw keys before decoding can discard unknown nested fields.
const exactKeys = (value: unknown, keys: ReadonlyArray<string>): value is Record<string, unknown> =>
  typeof value === "object" &&
  value !== null &&
  !Array.isArray(value) &&
  Object.keys(value).length === keys.length &&
  Object.keys(value).every((key) => keys.includes(key));
export const CodingNativeTranscriptInput = Schema.Unknown.check(
  Schema.makeFilter(
    (value) =>
      exactKeys(value, Object.keys(NativeObservation.fields)) &&
      exactKeys(value.reservation, Object.keys(CodingThreadReservationInput.fields)) &&
      (value.message === null ||
        exactKeys(value.message, ["id", "role", "text", "phase", "complete"])),
  ),
).pipe(Schema.decodeTo(NativeObservation));
export const CodingNativeTranscriptResult = Schema.Struct({
  thread_id: ThreadId,
  sequence: Schema.Int,
  revision: Digest,
  outcome: CodingNativeOutcome,
});
export type CodingNativeTranscriptInput = typeof CodingNativeTranscriptInput.Type;
export const CodingNativeCheckpoint = Schema.Struct({
  claim: Digest,
  intentDigest: Digest,
  nativeThreadId: NativeId,
  nativeTurnId: NativeId,
  sequence: Schema.Int,
  revision: Digest,
  outcome: CodingNativeOutcome,
});
export type CodingNativeCheckpoint = typeof CodingNativeCheckpoint.Type;
export class CodingNativeTranscriptUnavailable extends Schema.TaggedError<
  CodingNativeTranscriptUnavailable,
  { readonly brand: unique symbol }
>()("CodingNativeTranscriptUnavailable", {}) {
  override get message(): string {
    return "The Coding conversation could not be synchronized. Check its saved status in iCore.";
  }
}
