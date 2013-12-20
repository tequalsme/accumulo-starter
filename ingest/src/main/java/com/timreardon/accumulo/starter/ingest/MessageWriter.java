package com.timreardon.accumulo.starter.ingest;

import static com.timreardon.accumulo.starter.common.Constants.DOCUMENT_COLUMN_FAMILY;
import static com.timreardon.accumulo.starter.common.Constants.EMPTY_COL_VIS;
import static com.timreardon.accumulo.starter.common.Constants.EMPTY_TEXT;
import static com.timreardon.accumulo.starter.common.Constants.EMPTY_VALUE;
import static com.timreardon.accumulo.starter.common.Constants.FIELD_COLUMN_FAMILY;
import static com.timreardon.accumulo.starter.common.Constants.MIN_CHAR_STRING;
import static org.apache.commons.lang.StringUtils.isEmpty;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import com.timreardon.accumulo.starter.common.domain.Message;

/*
 * enron table:
 * R          CF             CQ                      V
 * uuid       f              fieldName\x0fieldValue  empty
 * uuid       d              empty                   document
 * index table:
 * R          CF             CQ                      V
 * fieldValue fieldName      uuid                    empty
 */
public class MessageWriter {
    public static final long DEFAULT_MAX_MUTATION_BUFFER_SIZE = 50 * 1024 * 1024; // 50MB
    public static final int DEFAULT_MAX_LATENCY = 60 * 1000; // 1 minute
    public static final int DEFAULT_NUM_WRITE_THREADS = 2;

    private final MultiTableBatchWriter multiTableBatchWriter;
    private final String table;
    private final String indexTable;

    public MessageWriter(Connector connector, String table, String indexTable) throws AccumuloException,
            AccumuloSecurityException {
        this(connector, table, indexTable, DEFAULT_MAX_MUTATION_BUFFER_SIZE, DEFAULT_MAX_LATENCY,
                DEFAULT_NUM_WRITE_THREADS);
    }

    public MessageWriter(Connector connector, String table, String indexTable, long maxMemory, long maxLatency,
            int maxWriteThreads) throws AccumuloException, AccumuloSecurityException {
        multiTableBatchWriter = connector.createMultiTableBatchWriter(maxMemory, maxLatency, maxWriteThreads);
        this.table = table;
        this.indexTable = indexTable;

        createTables(connector);
    }

    public void close() throws MutationsRejectedException {
        this.multiTableBatchWriter.close();
    }

    private void createTables(Connector connector) throws AccumuloException, AccumuloSecurityException {
        TableOperations tops = connector.tableOperations();

        try {
            if (!tops.exists(table)) {
                tops.create(table);
            }

            if (!tops.exists(indexTable)) {
                tops.create(indexTable);
            }
        } catch (TableExistsException e) {
            // shouldn't happen as we check for table existence prior to each create() call
            throw new AccumuloException(e);
        }
    }

    public void write(Iterable<Message> messages) throws AccumuloException, AccumuloSecurityException {
        try {
            for (Message message : messages) {
                Mutation mutation = new Mutation(message.getId());

                writeField(mutation, message, "FROM", message.getFrom(), true);

                if (message.getTo() != null) {
                    writeField(mutation, message, "TO", message.getToAsString(), false);
                    for (String s : message.getTo()) {
                        writeIndex(message, "TO", s);
                    }
                }

                if (message.getCc() != null) {
                    writeField(mutation, message, "CC", message.getCcAsString(), true);
                    for (String s : message.getCc()) {
                        writeIndex(message, "CC", s);
                    }
                }

                if (message.getBcc() != null) {
                    writeField(mutation, message, "BCC", message.getBccAsString(), true);
                    for (String s : message.getBcc()) {
                        writeIndex(message, "BCC", s);
                    }
                }

                if (message.getSubject() != null) {
                    writeField(mutation, message, "SUBJECT", message.getSubject(), true);
                    for (String s : message.getSubjectTokens()) {
                        writeIndex(message, "SUBJECT", s);
                    }
                }

                writeField(mutation, message, "MAILBOX", message.getMailbox(), true);
                writeField(mutation, message, "FOLDER", message.getFolder(), false);
                writeField(mutation, message, "FILENAME", message.getFilename(), false);

                for (String w : message.getBodyTokens()) {
                    writeField(mutation, message, "CONTENT", w, true);
                }

                mutation.put(DOCUMENT_COLUMN_FAMILY, EMPTY_TEXT, EMPTY_COL_VIS, message.getTimestamp(),
                        new Value(message.getRawBytes()));

                multiTableBatchWriter.getBatchWriter(table).addMutation(mutation);
            }
        } catch (TableNotFoundException e) {
            // why isn't TableNotFoundException a RuntimeException?
            throw new AccumuloException(e);
        }
    }

    private void writeField(Mutation mutation, Message message, String fieldName, String fieldValue, boolean isIndexed)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (isEmpty(fieldValue)) {
            return;
        }

        mutation.put(FIELD_COLUMN_FAMILY, fieldName + MIN_CHAR_STRING + fieldValue, EMPTY_COL_VIS, message.getTimestamp(),
                EMPTY_VALUE);

        if (isIndexed) {
            writeIndex(message, fieldName, fieldValue);
        }
    }

    private void writeIndex(Message message, String fieldName, String fieldValue) throws AccumuloException,
            AccumuloSecurityException, TableNotFoundException {
        if (isEmpty(fieldValue)) {
            return;
        }

        Mutation indexMutation = new Mutation(fieldValue);
        indexMutation.put(fieldName, message.getId(), EMPTY_COL_VIS, message.getTimestamp(), EMPTY_VALUE);
        multiTableBatchWriter.getBatchWriter(indexTable).addMutation(indexMutation);
    }
}
