import * as Schema from "effect/Schema";
import { IsoDateTime, ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const StoredProjectBindingInput = Schema.Struct({ projectId: ProjectId });
export type StoredProjectBindingInput = typeof StoredProjectBindingInput.Type;
export const StoredProjectBindingResult = Schema.NullOr(
  Schema.Struct({
    project_id: ProjectId,
    workspace_root: TrimmedNonEmptyString,
    folders: Schema.Array(TrimmedNonEmptyString).check(Schema.isMaxLength(128)),
    created_at: IsoDateTime,
    deleted_at: Schema.NullOr(IsoDateTime),
  }),
);
export class StoredProjectBindingUnavailable extends Schema.TaggedError<
  StoredProjectBindingUnavailable,
  { readonly brand: unique symbol }
>()("StoredProjectBindingUnavailable", {}) {
  override get message(): string {
    return "The Coding project could not be checked. Refresh its connection.";
  }
}
