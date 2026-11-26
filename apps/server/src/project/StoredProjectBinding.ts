import {
  StoredProjectBindingUnavailable,
  type StoredProjectBindingInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { ProjectStoreV2 } from "../orchestration-v2/ProjectStore.ts";

// Read stored metadata only: ordinary project hydration can scan folders and write metadata.
export const read = Effect.fn("StoredProjectBinding.read")(
  function* (input: StoredProjectBindingInput) {
    const projects = yield* ProjectStoreV2;
    const stored = yield* projects.get(input.projectId, { includeDeleted: true });
    if (Option.isNone(stored)) return null;
    const row = stored.value;
    return {
      project_id: row.projectId,
      workspace_root: row.workspaceRoot,
      folders: row.trackedFolders.map((folder) => folder.path),
      created_at: row.createdAt,
      deleted_at: row.deletedAt,
    };
  },
  (effect) => effect.pipe(Effect.mapError(() => new StoredProjectBindingUnavailable())),
);
