import * as Schema from "effect/Schema";
import { ProjectId } from "./baseSchemas.ts";
import { ProviderDriverKind, ProviderInstanceId } from "./providerInstance.ts";
import { RuntimeMode } from "./providerPolicy.ts";

export const ProviderLaunchBindingInput = Schema.Struct({
  projectId: ProjectId,
  instanceId: ProviderInstanceId,
  model: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(200)),
  runtimeMode: RuntimeMode,
});
export type ProviderLaunchBindingInput = typeof ProviderLaunchBindingInput.Type;

export const ProviderLaunchBindingResult = Schema.Struct({
  instance: ProviderInstanceId,
  driver: ProviderDriverKind,
  model: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(200)),
  runtime_mode: RuntimeMode,
  revision: Schema.String.check(
    Schema.isMinLength(64),
    Schema.isMaxLength(64),
    Schema.isPattern(/^[a-f0-9]{64}$/),
  ),
  available: Schema.Literal(true),
});
export type ProviderLaunchBindingResult = typeof ProviderLaunchBindingResult.Type;

export const ProviderModelAccessInput = Schema.Struct({
  ...ProviderLaunchBindingInput.fields,
  projectId: ProjectId.check(Schema.isPattern(/^icore-[a-f0-9]{48}$/)),
  revision: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
});
export type ProviderModelAccessInput = typeof ProviderModelAccessInput.Type;
export const ProviderModelAccessResult = Schema.Struct({
  binding: ProviderLaunchBindingResult,
  accessToken: Schema.String.check(Schema.isPattern(/^[\x21-\x7e]{1,32768}$/)),
  expiresAt: Schema.Finite.check(Schema.isGreaterThan(0)),
  kind: Schema.Literal("chatgpt-managed"),
  resource: Schema.Literal("https://api.openai.com/v1"),
});
export type ProviderModelAccessResult = typeof ProviderModelAccessResult.Type;

export class ProviderLaunchBindingUnavailable extends Schema.TaggedError<
  ProviderLaunchBindingUnavailable,
  { readonly brand: unique symbol }
>()("ProviderLaunchBindingUnavailable", {}) {
  override get message(): string {
    return "The selected endpoint changed or is not ready. Review its connection and choices again.";
  }
}
