import * as Schema from "effect/Schema";
import { IsoDateTime, ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { RuntimeMode } from "./providerPolicy.ts";

const Digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const CodingThreadReservationInput = Schema.Struct({
  claim: Digest,
  intentDigest: Digest,
  projectId: ProjectId.check(Schema.isPattern(/^icore-[a-f0-9]{48}$/)),
  title: TrimmedNonEmptyString.check(Schema.isMaxLength(120)),
  instanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString.check(Schema.isMaxLength(200)),
  runtimeMode: RuntimeMode,
});
export type CodingThreadReservationInput = typeof CodingThreadReservationInput.Type;
export const CodingThreadReservationResult = Schema.Struct({
  claim: Digest,
  intent_digest: Digest,
  project_id: ProjectId,
  thread_id: ThreadId,
  created_at: IsoDateTime,
});
export class CodingThreadReservationUnavailable extends Schema.TaggedError<
  CodingThreadReservationUnavailable,
  { readonly brand: unique symbol }
>()("CodingThreadReservationUnavailable", {}) {
  override get message(): string {
    return "The new Coding conversation could not be reserved. Review its status in iCore.";
  }
}
