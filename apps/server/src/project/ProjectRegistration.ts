import {
  CommandId,
  ProjectRegistrationUnavailable,
  type ProjectRegistrationInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { ProjectService } from "./ProjectService.ts";
import { registrationAuthority } from "./ProjectRegistrationAuthority.ts";

export const register = Effect.fn("ProjectRegistration.register")(
  function* (input: ProjectRegistrationInput) {
    const projects = yield* ProjectService;
    const project = yield* projects.create({
      commandId: CommandId.make("create-" + input.projectId),
      projectId: input.projectId,
      title: input.title,
      workspaceRoot: input.workspaceRoot,
      trackedFolders: input.folders.map((path) => ({
        path,
        name: path.split("/").at(-1)!.slice(0, 128),
      })),
      registrationAuthority,
    });
    return {
      project_id: project.id,
      workspace_root: project.workspaceRoot,
      folders: (project.trackedFolders ?? []).map((folder) => folder.path),
      created_at: project.createdAt,
      deleted_at: project.deletedAt,
    };
  },
  (effect) => effect.pipe(Effect.mapError(() => new ProjectRegistrationUnavailable())),
);
