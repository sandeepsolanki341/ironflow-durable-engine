package io.ironflow.api.dto;

import java.util.List;

/**
 * A single page of results plus the cursor the client needs to ask for the next one.
 *
 * <p>Uses offset/limit paging. Offset paging is chosen over keyset here because the list is
 * an operator-facing dashboard with status filters and free-text search, where "jump to page
 * 5" and a total count are worth more than the constant-time deep-pagination that keyset
 * would buy - and these result sets are bounded by the filters, not the full event table.</p>
 *
 * @param items      the rows on this page
 * @param page       zero-based page index that was returned
 * @param size       page size that was requested
 * @param totalItems total rows matching the filter across all pages
 * @param totalPages total number of pages at this size (at least 1, even when empty)
 * @param <T>        row type
 */
public record PageResponse<T>(
        List<T> items,
        int page,
        int size,
        long totalItems,
        int totalPages) {

    public static <T> PageResponse<T> of(List<T> items, int page, int size, long totalItems) {
        int totalPages = size <= 0 ? 1 : (int) Math.max(1, Math.ceil((double) totalItems / size));
        return new PageResponse<>(items, page, size, totalItems, totalPages);
    }
}
