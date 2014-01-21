package com.timreardon.accumulo.starter.query.support;

import static org.apache.commons.lang.Validate.notNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.calrissian.mango.collect.FluentCloseableIterable;

import com.google.common.base.Function;

/**
 * Helper methods for working with a BatchScanner as an CloseableIterable so that it can be properly closed upon completion.
 */
public final class BatchScannerIterables {
    private BatchScannerIterables() {
        // empty
    }

    /**
     * Creates a {@link CloseableIterable} from a BatchScanner, autoclosing the iterator when exhausted.
     * <p>
     * This method is necessary because BatchScanner does not implement Closeable, yet defines a close() method that must be called to free thread resources.
     * 
     * @param bs BatchScanner to wrap
     * @return {@link CloseableIterable}
     * @see CloseableIterables#autoClose(CloseableIterable)
     */
    @SuppressWarnings("unchecked")
    public static final CloseableIterable<Entry<Key, Value>> wrapBatchScanner(final BatchScanner bs) {
        notNull(bs);
        if (bs instanceof CloseableIterable)
            return (CloseableIterable<Entry<Key, Value>>) bs;

        return CloseableIterables.autoClose(new FluentCloseableIterable<Entry<Key, Value>>() {
            @Override
            protected void doClose() throws IOException {
                bs.close();
            }

            @Override
            protected Iterator<Entry<Key, Value>> retrieveIterator() {
                return bs.iterator();
            }
        });
    }

    /**
     * Transforms Entry<Key, Value> (provided by BatchScanner) using the supplied Function, while wrapping the BatchScanner so that it is properly closed upon
     * completion.
     * 
     * @param bs BatchScanner
     * @return CloseableIterable<T>
     * @see #wrapBatchScanner(BatchScanner)
     */
    public static final <T> CloseableIterable<T> transform(BatchScanner bs, Function<Entry<Key, Value>, T> function) {
        return CloseableIterables.transform(wrapBatchScanner(bs), function);
    }
}
