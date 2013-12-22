package com.timreardon.accumulo.starter.query.impl;

import static com.timreardon.accumulo.starter.common.Constants.FIELD_COLUMN_FAMILY;
import static com.timreardon.accumulo.starter.common.Constants.MIN_CHAR_STRING;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.apache.accumulo.core.Constants.NO_AUTHS;
import static org.apache.commons.lang.Validate.notEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.query.QueryService;

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

    @Override
    public List<Message> query(String term) throws QueryException {
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
                return emptyList();
            }

            BatchScanner scanner = connector.createBatchScanner(tableName,
                    NO_AUTHS, numQueryThreads);
            scanner.setRanges(ranges);
            scanner.fetchColumnFamily(new Text(FIELD_COLUMN_FAMILY));

            IteratorSetting cfg = new IteratorSetting(21, WholeRowIterator.class); // must be 21 or higher
            scanner.addScanIterator(cfg);

            List<Message> messages = new ArrayList<Message>();
            for (Entry<Key, Value> entry : scanner) {
                Message message = new Message();
                message.setId(entry.getKey().getRow().toString());
                message.setTimestamp(entry.getKey().getTimestamp());
                SortedMap<Key, Value> map = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
                // TODO abstract this out somewhere
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
                messages.add(message);
            }
            scanner.close();

            return messages;

        } catch (TableNotFoundException e) {
            throw new QueryException(e);
        } catch (IOException e) {
            throw new QueryException(e);
        }
    }
}
