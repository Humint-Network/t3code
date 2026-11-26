import * as Schema from "effect/Schema";
import { ProviderInstanceId } from "./providerInstance.ts";

const Id = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
export const ProviderSetupTransportBegin = Schema.Struct({ instanceId: ProviderInstanceId });
export type ProviderSetupTransportBegin = typeof ProviderSetupTransportBegin.Type;
export const ProviderSetupTransportLease = Schema.Struct({ lease: Id });
export type ProviderSetupTransportLease = typeof ProviderSetupTransportLease.Type;
export const ProviderSetupTransportRequest = Schema.Struct({
  lease: Id,
  id: Id,
  instanceId: ProviderInstanceId,
  expiresAt: Schema.Finite.check(Schema.isGreaterThan(0)),
  method: Schema.Literals(["GET", "POST"]),
  url: Schema.String.check(Schema.isMaxLength(2048)),
  headers: Schema.Record(Schema.String, Schema.String),
  body: Schema.String.check(Schema.isMaxLength(65536)),
});
export type ProviderSetupTransportRequest = typeof ProviderSetupTransportRequest.Type;
export const ProviderSetupTransportResponse = Schema.Struct({
  status: Schema.Int.check(Schema.isBetween({ minimum: 200, maximum: 599 })),
  body: Schema.String.check(Schema.isMaxLength(1048576)),
});
export type ProviderSetupTransportResponse = typeof ProviderSetupTransportResponse.Type;
export const ProviderSetupTransportComplete = Schema.Struct({
  lease: Id,
  id: Id,
  response: Schema.NullOr(ProviderSetupTransportResponse),
});
export type ProviderSetupTransportComplete = typeof ProviderSetupTransportComplete.Type;
export class ProviderSetupTransportUnavailable extends Schema.TaggedError<
  ProviderSetupTransportUnavailable,
  { readonly brand: unique symbol }
>()("ProviderSetupTransportUnavailable", {}) {
  override get message(): string {
    return "The account setup connection is unavailable. Return to iCore to try again.";
  }
}

export const ProviderNativeEngine = Schema.Struct({
  revision: Id,
  version: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._+-]{1,100}$/)),
});
export type ProviderNativeEngine = typeof ProviderNativeEngine.Type;
export const ProviderNativeEngineAttestation = Schema.Struct({
  lease: Id,
  engine: ProviderNativeEngine,
});
export type ProviderNativeEngineAttestation = typeof ProviderNativeEngineAttestation.Type;

export const ProviderNativeEngineCheck = Schema.Struct({ lease: Id, check: Id });
export type ProviderNativeEngineCheck = typeof ProviderNativeEngineCheck.Type;
export const ProviderNativeEngineStatus = Schema.Struct({
  check: Id,
  phase: Schema.Literals(["pending", "ready", "unavailable"]),
});
export type ProviderNativeEngineStatus = typeof ProviderNativeEngineStatus.Type;
