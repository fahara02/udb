package dev.udb.client;

import java.util.List;

/**
 * Typed read-your-writes fence carried by a subsequent read. Mirrors the Rust
 * {@code ReadFence} serde shape: {@code max_wait_ms} is always serialized while
 * {@code min_outbox_lsn} and {@code projection_task_ids} are omitted when empty.
 */
public record ReadFence(String minOutboxLsn, List<String> projectionTaskIds, long maxWaitMs) {
  public static final long DEFAULT_MAX_WAIT_MS = 2500;

  public ReadFence {
    minOutboxLsn = minOutboxLsn == null ? "" : minOutboxLsn;
    projectionTaskIds =
        projectionTaskIds == null ? List.of() : List.copyOf(projectionTaskIds);
    maxWaitMs = Math.max(0, maxWaitMs);
  }

  public static ReadFence fromReceipt(WriteReceipt receipt) {
    return fromReceipt(receipt, DEFAULT_MAX_WAIT_MS);
  }

  public static ReadFence fromReceipt(WriteReceipt receipt, long maxWaitMs) {
    if (receipt == null) {
      return new ReadFence("", List.of(), maxWaitMs);
    }
    return new ReadFence(receipt.sourceLsn(), receipt.projectionTaskIds(), maxWaitMs);
  }

  public boolean isEmpty() {
    return minOutboxLsn.isEmpty() && projectionTaskIds.isEmpty();
  }

  public String toJson() {
    StringBuilder out = new StringBuilder();
    out.append("{\"max_wait_ms\":").append(maxWaitMs);
    if (!minOutboxLsn.isEmpty()) {
      out.append(",\"min_outbox_lsn\":\"").append(WriteReceipt.escape(minOutboxLsn)).append('"');
    }
    if (!projectionTaskIds.isEmpty()) {
      out.append(",\"projection_task_ids\":")
          .append(WriteReceipt.stringArrayJson(projectionTaskIds));
    }
    return out.append('}').toString();
  }
}
