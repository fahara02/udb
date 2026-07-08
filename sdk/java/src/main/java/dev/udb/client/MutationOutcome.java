package dev.udb.client;

import com.udb.entity.v1.MutationResponse;
import java.util.Objects;

/**
 * Ergonomic view over a {@link MutationResponse} returned by an upsert/delete.
 *
 * <p>Surfaces the broker's {@code was_duplicate} flag — {@code true} when the
 * write was collapsed as a durable-idempotency replay rather than applied as a
 * fresh mutation — alongside the typed {@link WriteReceipt} and the raw response
 * (kept available for everything else: {@code mutation_id}, {@code affected_rows},
 * warnings, metadata, …).
 *
 * <p>The underlying flag also stays reachable directly as
 * {@code response.getWasDuplicate()}; this wrapper simply makes the
 * replay-vs-fresh distinction discoverable at the facade layer.
 */
public record MutationOutcome(
    boolean wasDuplicate, WriteReceipt writeReceipt, MutationResponse response) {

  public MutationOutcome {
    Objects.requireNonNull(writeReceipt, "writeReceipt");
    Objects.requireNonNull(response, "response");
  }

  /** Derive an outcome from a raw upsert/delete {@link MutationResponse}. */
  public static MutationOutcome of(MutationResponse response) {
    Objects.requireNonNull(response, "response");
    return new MutationOutcome(
        response.getWasDuplicate(),
        WriteReceipt.fromJson(response.getWriteReceiptJson()),
        response);
  }
}
