// @effect-diagnostics-next-line nodeBuiltinImport:off -- deterministic IDs for bounded public observations.
import * as NodeCrypto from "node:crypto";
import {
  CommandId,
  MessageId,
  TurnItemId,
  type CodingNativeTranscriptInput,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2TurnItem,
  type OrchestrationV2ConversationMessage,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

type WithoutId<E> = E extends unknown ? Omit<E, "id"> : never;
type EventInput = WithoutId<OrchestrationV2DomainEvent>;
type Projection = Pick<
  OrchestrationV2ThreadProjection,
  "thread" | "messages" | "turnItems" | "runs" | "providerSessions" | "providerThreads"
>;
const hash = (value: unknown) =>
  NodeCrypto.createHash("sha256").update(JSON.stringify(value)).digest("hex");
export const revision = (input: CodingNativeTranscriptInput) =>
  hash([
    "icore-native-observation-v1",
    input.reservation.claim,
    input.reservation.intentDigest,
    input.reservation.projectId,
    input.reservation.title,
    input.reservation.instanceId,
    input.reservation.model,
    input.reservation.runtimeMode,
    input.nativeThreadId,
    input.nativeTurnId,
    input.sequence,
    input.previousRevision,
    input.message === null
      ? null
      : [
          input.message.id,
          input.message.role,
          input.message.text,
          input.message.phase,
          input.message.complete,
        ],
    input.outcome,
  ]);
export const commandId = (input: CodingNativeTranscriptInput) =>
  CommandId.make("icore-native-" + revision(input));
function refuse(): never {
  throw new Error("The native transcript observation conflicts with its reserved conversation.");
}

export function plan(
  projection: Projection,
  input: CodingNativeTranscriptInput,
  reservedAt: string,
  now: DateTime.Utc,
): ReadonlyArray<EventInput> {
  const thread = projection.thread,
    saved = thread.codingNative,
    r = input.reservation;
  if (
    thread.id !== "icore-" + r.claim ||
    thread.projectId !== r.projectId ||
    thread.title !== r.title ||
    thread.modelSelection.instanceId !== r.instanceId ||
    thread.modelSelection.model !== r.model ||
    thread.runtimeMode !== r.runtimeMode ||
    thread.createdBy !== "user" ||
    thread.creationSource !== "server" ||
    DateTime.formatIso(thread.createdAt) !== reservedAt ||
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    thread.activeProviderThreadId !== null ||
    thread.forkedFrom !== null ||
    projection.runs.length !== 0 ||
    projection.providerThreads.length !== 0 ||
    projection.providerSessions.length !== 0
  )
    refuse();
  if (saved === undefined) {
    if (
      input.sequence !== 0 ||
      input.previousRevision !== null ||
      projection.messages.length !== 0 ||
      projection.turnItems.length !== 0 ||
      input.message?.role !== "user" ||
      input.message.complete !== true ||
      input.message.phase !== null ||
      input.outcome !== "active"
    )
      refuse();
  } else if (
    saved.claim !== r.claim ||
    saved.intentDigest !== r.intentDigest ||
    saved.nativeThreadId !== input.nativeThreadId ||
    saved.nativeTurnId !== input.nativeTurnId ||
    saved.sequence + 1 !== input.sequence ||
    saved.revision !== input.previousRevision ||
    saved.outcome !== "active" ||
    input.message?.role === "user"
  )
    refuse();
  const events: EventInput[] = [];
  const envelope = { threadId: thread.id, occurredAt: now, providerInstanceId: r.instanceId };
  const emitItem = (payload: OrchestrationV2TurnItem) =>
    events.push({ ...envelope, type: "turn-item.updated", payload });
  const nextOrdinal = projection.turnItems.reduce((n, item) => Math.max(n, item.ordinal + 1), 0);
  const terminal = input.outcome !== "active";
  const message = input.message;
  if (message !== null) {
    if (new TextEncoder().encode(message.text).length > 262144) refuse();
    const key = hash([r.claim, message.id]);
    const id = MessageId.make("icore-native-message-" + key);
    const itemId = TurnItemId.make("icore-native-item-" + key);
    const prior = projection.messages.find((m) => m.id === id);
    const priorItem = projection.turnItems.find((item) => item.id === itemId);
    if ((prior === undefined) !== (priorItem === undefined)) refuse();
    if (
      prior !== undefined &&
      (prior.role !== message.role ||
        prior.runId !== null ||
        prior.nodeId !== null ||
        (!prior.streaming && (prior.text !== message.text || !message.complete)) ||
        priorItem?.type !== "assistant_message" ||
        (!priorItem.streaming && (priorItem.phase ?? null) !== message.phase))
    )
      refuse();
    const bytes = projection.messages.reduce(
      (n, m) => n + (m.id === id ? 0 : new TextEncoder().encode(m.text).length),
      0,
    );
    if (bytes + new TextEncoder().encode(message.text).length > 8 * 1024 * 1024) refuse();
    const createdAt = prior?.createdAt ?? now;
    const record: OrchestrationV2ConversationMessage = {
      id,
      threadId: thread.id,
      runId: null,
      nodeId: null,
      role: message.role,
      text: message.text,
      attachments: [],
      streaming: !message.complete && !terminal,
      createdAt,
      updatedAt: now,
      createdBy: message.role === "user" ? "user" : "agent",
      creationSource: "server",
    };
    events.push({ ...envelope, type: "message.updated", payload: record });
    const base = {
      id: itemId,
      threadId: thread.id,
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: priorItem?.ordinal ?? nextOrdinal,
      status: message.complete
        ? ("completed" as const)
        : terminal
          ? ("interrupted" as const)
          : ("running" as const),
      title: null,
      startedAt: createdAt,
      completedAt: message.complete || terminal ? now : null,
      updatedAt: now,
    };
    emitItem(
      message.role === "user"
        ? {
            ...base,
            type: "user_message",
            createdBy: "user",
            creationSource: "server",
            messageId: id,
            inputIntent: "turn_start",
            text: message.text,
            attachments: [],
          }
        : {
            ...base,
            type: "assistant_message",
            messageId: id,
            text: message.text,
            streaming: record.streaming,
            ...(message.phase === null ? {} : { phase: message.phase }),
          },
    );
  }
  if (terminal) {
    for (const item of projection.turnItems) {
      if (
        item.type !== "assistant_message" ||
        !item.streaming ||
        item.messageId === events.find((e) => e.type === "message.updated")?.payload.id
      )
        continue;
      const prior = projection.messages.find((m) => m.id === item.messageId);
      if (prior === undefined) refuse();
      events.push({
        ...envelope,
        type: "message.updated",
        payload: { ...prior, streaming: false, updatedAt: now },
      });
      emitItem({
        ...item,
        streaming: false,
        status: "interrupted",
        completedAt: now,
        updatedAt: now,
      });
    }
    const notices = {
      completed: "Native work completed in iCore.",
      failed: "Native work failed. Check the saved outcome in iCore.",
      stopped: "Native work was stopped in iCore.",
      interrupted: "Native work was interrupted. Check the saved outcome in iCore.",
      uncertain:
        "The native work outcome is uncertain. Check it in iCore before starting more work.",
      recovery_required:
        "This native run is no longer connected. Its saved messages do not confirm completion. Check it in iCore.",
    };
    emitItem({
      id: TurnItemId.make("icore-native-outcome-" + r.claim),
      threadId: thread.id,
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      ordinal: nextOrdinal + (message === null ? 0 : 1),
      status: input.outcome === "completed" ? "completed" : "interrupted",
      title: null,
      startedAt: now,
      completedAt: now,
      updatedAt: now,
      type: "system_notice",
      message: notices[input.outcome],
      level: input.outcome === "completed" ? "info" : "warning",
    });
  }
  events.push({
    ...envelope,
    type: "thread.metadata-updated",
    payload: {
      ...thread,
      updatedAt: now,
      codingNative: {
        claim: r.claim,
        intentDigest: r.intentDigest,
        nativeThreadId: input.nativeThreadId,
        nativeTurnId: input.nativeTurnId,
        sequence: input.sequence,
        revision: revision(input),
        outcome: input.outcome,
      },
    },
  });
  return events;
}
