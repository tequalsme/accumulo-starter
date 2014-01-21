package com.timreardon.accumulo.starter.query.impl;

import static com.timreardon.accumulo.starter.common.Constants.FIELD_COLUMN_FAMILY;
import static com.timreardon.accumulo.starter.common.Constants.MIN_CHAR_STRING;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singleton;
import static org.apache.accumulo.core.Constants.NO_AUTHS;
import static org.apache.commons.lang.Validate.notEmpty;
import static org.calrissian.mango.collect.CloseableIterables.wrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.calrissian.mango.collect.CloseableIterable;

import com.google.common.base.Function;
import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.query.QueryService;
import com.timreardon.accumulo.starter.query.support.BatchScannerIterables;

public class QueryServiceImpl implements QueryService {
    public static final int DEFAULT_NUM_QUERY_THREADS = 8;

    private final Connector connector;
    private final String tableName;
    private final String indexTableName;

    private int numQueryThreads = DEFAULT_NUM_QUERY_THREADS;

    public QueryServiceImpl(Connector connector, String tableName, String indexTableName) {
        this.connector = connector;
        this.tableName = tableName;
        this.indexTableName = indexTableName;
    }

    public void setNumQueryThreads(int numQueryThreads) {
        this.numQueryThreads = numQueryThreads;
    }

    @SuppressWarnings("unchecked")
    @Override
    public CloseableIterable<Message> query(String term) throws QueryException {
        notEmpty(term);
        
        try {
            /*
             * enron table:
             * R          CF             CQ                      V
             * uuid       f              fieldName\x0fieldValue  empty
             * uuid       d              empty                   document
             * index table:
             * R          CF             CQ                      V
             * fieldValue fieldName      uuid                    empty
             */

            BatchScanner indexScanner = connector.createBatchScanner(indexTableName,
                    NO_AUTHS, numQueryThreads);
            indexScanner.setRanges(singleton(new Range(term)));
            
            Collection<Range> ranges = new ArrayList<Range>();
            for (Entry<Key, Value> entry : indexScanner) {
                ranges.add(new Range(entry.getKey().getColumnQualifier().toString()));
            }
            indexScanner.close();
            
            if (ranges.isEmpty()) {
                return wrap(EMPTY_LIST);
            }

            BatchScanner scanner = connector.createBatchScanner(tableName,
                    NO_AUTHS, numQueryThreads);
            scanner.setRanges(ranges);
            scanner.fetchColumnFamily(new Text(FIELD_COLUMN_FAMILY));

            IteratorSetting cfg = new IteratorSetting(21, WholeRowIterator.class); // must be 21 or higher
            scanner.addScanIterator(cfg);
            
            return transform(scanner);

        } catch (TableNotFoundException e) {
            throw new QueryException(e);
        }
    }
    
    /**
     * Transforms Entry<Key, Value> (from BatchScanner) to Message objects.
     * 
     * @param bs
     *            BatchScanner
     * @return CloseableIterable<Message>
     */
    protected CloseableIterable<Message> transform(BatchScanner bs) {
        return BatchScannerIterables.transform(bs, new Function<Entry<Key, Value>, Message>() {
            @Override
            public Message apply(Entry<Key, Value> entry) {
                try {
                    Message message = new Message();
                    message.setId(entry.getKey().getRow().toString());
                    message.setTimestamp(entry.getKey().getTimestamp());
                    SortedMap<Key, Value> map = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
                    // TODO abstract this out somewhere?
                    for (Entry<Key, Value> e2 : map.entrySet()) {
                        String[] a = e2.getKey().getColumnQualifier().toString().split(MIN_CHAR_STRING);
                        String fieldName = a[0];
                        String fieldValue = a[1];
                        if (fieldName.equals("FROM")) {
                            message.setFrom(fieldValue);
                        } else if (fieldName.equals("TO")) {
                            message.addTo(fieldValue);
                        } else if (fieldName.equals("CC")) {
                            message.addCc(fieldValue);
                        } else if (fieldName.equals("BCC")) {
                            message.addBcc(fieldValue);
                        } else if (fieldName.equals("SUBJECT")) {
                            message.setSubject(fieldValue);
                        } else if (fieldName.equals("MAILBOX")) {
                            message.setMailbox(fieldValue);
                        } else if (fieldName.equals("FOLDER")) {
                            message.setFolder(fieldValue);
                        } else if (fieldName.equals("FILENAME")) {
                            message.setFilename(fieldValue);
                        }
                    }
                    
                    return message;
                } catch (IOException e) {
                    throw new QueryException(e);
                }
            }
        });
    }
}
