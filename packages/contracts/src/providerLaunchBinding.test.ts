import { describe, expect, it } from "vite-plus/test";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import {
  ProviderLaunchBindingInput,
  ProviderLaunchBindingResult,
} from "./providerLaunchBinding.ts";
import { WS_METHODS, WsRpcGroup } from "./rpc.ts";

describe("launch binding contract", () => {
  const input = {
    projectId: "project",
    instanceId: "codex",
    model: "model-a",
    runtimeMode: "approval-required",
  };
  const result = {
    instance: "codex",
    driver: "codex",
    model: "model-a",
    runtime_mode: "approval-required",
    revision: "a".repeat(64),
    available: true,
  };
  const decodeInput = Schema.decodeUnknownExit(ProviderLaunchBindingInput);
  const decodeResult = Schema.decodeUnknownExit(ProviderLaunchBindingResult);

  const rpc = WsRpcGroup.requests.get(WS_METHODS.serverGetProviderLaunchBinding);
  const decodeRpc = Schema.decodeUnknownExit(rpc!.payloadSchema);
  const decodeResultSync = Schema.decodeUnknownSync(ProviderLaunchBindingResult);

  it("requires an explicit mode and bounded model in the registered unary RPC", () => {
    expect(rpc).toBeDefined();
    expect(Exit.isSuccess(decodeRpc(input))).toBe(true);
    expect(Exit.isFailure(decodeInput({ ...input, runtimeMode: undefined }))).toBe(true);
    expect(
      Exit.isFailure(
        decodeInput({
          projectId: input.projectId,
          instanceId: input.instanceId,
          model: input.model,
        }),
      ),
    ).toBe(true);
    expect(Exit.isFailure(decodeInput({ ...input, runtimeMode: "default" }))).toBe(true);
    expect(Exit.isFailure(decodeInput({ ...input, model: "m".repeat(201) }))).toBe(true);
  });

  it("rejects absent, false and malformed revisions", () => {
    expect(Exit.isSuccess(decodeResult(result))).toBe(true);
    expect(Exit.isFailure(decodeResult({ ...result, available: false }))).toBe(true);
    expect(Exit.isFailure(decodeResult({ ...result, revision: undefined }))).toBe(true);
    expect(Exit.isFailure(decodeResult({ ...result, revision: "a".repeat(64) + "\n" }))).toBe(true);
    expect(Exit.isFailure(decodeResult({ ...result, revision: "x".repeat(64) }))).toBe(true);
  });

  it("projects only the six binding fields", () => {
    const decoded = decodeResultSync({
      ...result,
      credentials: "synthetic-secret",
      environment: { TOKEN: "synthetic-secret" },
    });
    expect(Object.keys(decoded).sort()).toEqual([
      "available",
      "driver",
      "instance",
      "model",
      "revision",
      "runtime_mode",
    ]);
  });
});
