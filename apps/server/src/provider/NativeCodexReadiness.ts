import {
  type ProviderNativeEngine,
  type ServerProvider,
  ProviderSetupError,
  type ProviderSetupTransportUnavailable,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { HttpClient } from "effect/http";
import { chatGptModels } from "./CodexChatGptModels.ts";
import { type makeCodexChatGptAuth } from "./CodexChatGptAuth.ts";

type Auth = Effect.Success<ReturnType<typeof makeCodexChatGptAuth>>;
export const check = Effect.fn("NativeCodexReadiness.check")(function* (input: {
  readonly base: ServerProvider;
  readonly auth: Pick<Auth, "read" | "access" | "profileKey" | "controller" | "http">;
  readonly engine: Effect.Effect<
    ProviderNativeEngine & { readonly check: string },
    ProviderSetupError | ProviderSetupTransportUnavailable
  >;
}) {
  const { base, auth } = input;
  const failed = {
    ...base,
    installed: false,
    status: "warning" as const,
    models: [],
    supportedRuntimeModes: [],
    auth: { status: "unauthenticated" as const },
    message: "Return to iCore to check the installed engine and account.",
  };
  return yield* Effect.gen(function* () {
    const engine = yield* input.engine;
    const saved = yield* auth.read;
    const installed = { ...failed, installed: true, version: engine.version };
    if (Option.isNone(saved) || !saved.value.scopes.includes("chatgpt.tokens.use.direct"))
      return installed;
    const revision = yield* auth.profileKey(saved.value);
    if (!auth.controller.withAccess) return installed;
    return yield* auth.controller
      .withAccess(
        Effect.gen(function* () {
          const credentials = yield* auth.access;
          if ((yield* auth.profileKey(credentials)) !== revision)
            return yield* new ProviderSetupError({
              instanceId: base.instanceId,
              operation: "models",
              detail: "The account changed.",
            });
          const models = yield* chatGptModels(credentials.accessToken, []).pipe(
            Effect.provideService(HttpClient.HttpClient, auth.http),
          );
          const latest = yield* auth.read;
          const engineAfter = yield* input.engine;
          if (
            Option.isNone(latest) ||
            (yield* auth.profileKey(latest.value)) !== revision ||
            engineAfter.check !== engine.check ||
            engineAfter.revision !== engine.revision ||
            engineAfter.version !== engine.version
          )
            return yield* new ProviderSetupError({
              instanceId: base.instanceId,
              operation: "models",
              detail: "The endpoint changed.",
            });
          return {
            ...installed,
            status: models.length ? ("ready" as const) : ("warning" as const),
            models,
            supportedRuntimeModes: ["approval-required" as const],
            auth: {
              status: "authenticated" as const,
              type: "chatgpt",
              label: "ChatGPT",
              subscriptionSharing: true,
              profileId: credentials.clientId,
            },
            message: models.length
              ? "Ready for reviewed work in iCore."
              : "No available models for this account.",
          };
        }),
      )
      .pipe(Effect.scoped);
  }).pipe(Effect.orElseSucceed(() => failed));
});
