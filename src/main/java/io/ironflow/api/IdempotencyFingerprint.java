package io.ironflow.api;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.TreeMap;

/**
 * Start-request idempotency fingerprinting.
 *
 * <h2>What "idempotent" has to mean here</h2>
 *
 * <p>The easy half is handled by {@code uq_wf_exec_business_key}: a duplicate insert fails,
 * we catch it, we return the existing execution. That covers a client retrying after a lost
 * response.</p>
 *
 * <p>The hard half is telling a genuine retry apart from a <em>key collision</em>. A caller
 * who reuses "order-123" across tenants, or whose retry carries a mutated payload, gets back
 * an execution running input they never sent - and believes their request succeeded. That is
 * a silent correctness failure, the kind that surfaces weeks later as "why did this order
 * ship the wrong items".</p>
 *
 * <p>So we fingerprint the input. Same key and same fingerprint is a retry: return the
 * existing execution, 200. Same key and different fingerprint is a collision: reject with
 * 409 and say so.</p>
 */
public final class IdempotencyFingerprint {

    private IdempotencyFingerprint() { }

    /**
     * Computes a stable fingerprint over the start request's semantic content.
     *
     * <p>Canonicalizes JSON so that logically identical input with different key ordering or
     * whitespace produces the same fingerprint. Without this, a client whose JSON library
     * reorders map keys between attempts would see its own retry rejected as a collision -
     * technically correct, operationally infuriating.</p>
     *
     * <p>Includes {@code workflowType}: the same business key on a different workflow type
     * is unambiguously a collision, not a retry.</p>
     */
    public static String of(String workflowType, JsonNode input) {
        try {
            String canonical = workflowType + "\u0000"
                    + (input == null ? "null" : canonicalize(input));
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot fingerprint start request", e);
        }
    }

    /** Recursively sorts object keys so serialization is order-independent. */
    private static String canonicalize(JsonNode node) {
        if (node.isObject()) {
            var sorted = new TreeMap<String, String>();
            node.fields().forEachRemaining(e ->
                    sorted.put(e.getKey(), canonicalize(e.getValue())));
            return sorted.toString();
        }
        if (node.isArray()) {
            var sb = new StringBuilder("[");
            node.forEach(child -> sb.append(canonicalize(child)).append(','));
            return sb.append(']').toString();
        }
        return node.asText();
    }
}
