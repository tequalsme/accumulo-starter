package com.timreardon.accumulo.starter.ingest;

import static com.timreardon.accumulo.starter.common.Constants.DOCUMENT_COLUMN_FAMILY;
import static com.timreardon.accumulo.starter.common.Constants.EMPTY_TEXT;
import static com.timreardon.accumulo.starter.common.Constants.EMPTY_VALUE;
import static com.timreardon.accumulo.starter.common.Constants.FIELD_COLUMN_FAMILY;
import static com.timreardon.accumulo.starter.common.Constants.NULL_BYTE;
import static org.apache.commons.lang.StringUtils.isEmpty;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.ingest.parser.MessageParser;

public class IngestMapper extends Mapper<Text, BytesWritable, Text, Mutation> {
    private MessageParser parser;
    private Text tableName;
    private Text indexTableName;
    
    private ColumnVisibility visibility = new ColumnVisibility();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        parser = new MessageParser();
        tableName = new Text(IngestConfiguration.getTableName(conf));
        indexTableName = new Text(IngestConfiguration.getIndexTableName(conf));
    }

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message message = parser.parse(value.getBytes(), key.toString());

        /*
         * enron table:
         * R          CF             CQ                      V
         * uuid       f              fieldName\x0fieldValue  empty
         * uuid       d              empty                   document
         * index table:
         * R          CF             CQ                      V
         * fieldValue fieldName      uuid                    empty
         */
        Mutation mutation = new Mutation(message.getId());

        writeField(context, mutation, message, "FROM", message.getFrom(), true);
        
        if (message.getTo() != null) {
            writeField(context, mutation, message, "TO", message.getToAsString(), false);
            for (String s : message.getTo()) {
                writeIndex(context, message, "TO", s);
            }
        }

        if (message.getCc() != null) {
            writeField(context, mutation, message, "CC", message.getCcAsString(), true);
            for (String s : message.getCc()) {
                writeIndex(context, message, "CC", s);
            }
        }

        if (message.getBcc() != null) {
            writeField(context, mutation, message, "BCC", message.getBccAsString(), true);
            for (String s : message.getBcc()) {
                writeIndex(context, message, "BCC", s);
            }
        }

        if (message.getSubject() != null) {
            writeField(context, mutation, message, "SUBJECT", message.getSubject(), true);
            for (String s : message.getSubjectTokens()) {
                writeIndex(context, message, "SUBJECT", s);
            }
        }
        
        writeField(context, mutation, message, "MAILBOX", message.getMailbox(), true);
        writeField(context, mutation, message, "FOLDER", message.getFolder(), false);
        writeField(context, mutation, message, "FILENAME", message.getFilename(), false);
        
        for (String w : message.getBodyTokens()) {
            writeField(context, mutation, message, "CONTENT", w, false);
        }
        
        mutation.put(DOCUMENT_COLUMN_FAMILY, EMPTY_TEXT, visibility, message.getTimestamp(), new Value(value.getBytes()));

        context.write(tableName, mutation);
        context.getCounter("ingest", "count").increment(1);
    }
    
    private void writeField(Context context, Mutation mutation, Message message, String fieldName, String fieldValue, boolean isIndexed) throws IOException, InterruptedException {
        if (isEmpty(fieldValue)) { 
            return;
        }
    
        mutation.put(FIELD_COLUMN_FAMILY, fieldName + NULL_BYTE + fieldValue, visibility, message.getTimestamp(), EMPTY_VALUE);

        if (isIndexed) {
            writeIndex(context, message, fieldName, fieldValue);
        }
    }
    
    private void writeIndex(Context context, Message message, String fieldName, String fieldValue) throws IOException, InterruptedException {
        if (isEmpty(fieldValue)) { 
            return;
        }
        
        Mutation indexMutation = new Mutation(fieldValue);
        indexMutation.put(fieldName, message.getId(), visibility, message.getTimestamp(), EMPTY_VALUE);
        context.write(indexTableName, indexMutation);
    }
}
