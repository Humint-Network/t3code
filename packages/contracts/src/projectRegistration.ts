import * as Schema from "effect/Schema";
import { ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const ProjectRegistrationInput = Schema.Struct({
  projectId: ProjectId,
  title: TrimmedNonEmptyString.check(Schema.isMaxLength(120)),
  workspaceRoot: TrimmedNonEmptyString.check(Schema.isMaxLength(4096)),
  folders: Schema.Array(TrimmedNonEmptyString.check(Schema.isMaxLength(4096))).check(
    Schema.isMaxLength(19),
  ),
});
export type ProjectRegistrationInput = typeof ProjectRegistrationInput.Type;
export class ProjectRegistrationUnavailable extends Schema.TaggedError<
  ProjectRegistrationUnavailable,
  { readonly brand: unique symbol }
>()("ProjectRegistrationUnavailable", {}) {
  override get message(): string {
    return "This project's Coding connection could not be registered. Review it in iCore.";
  }
}
