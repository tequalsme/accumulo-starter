package com.timreardon.accumulo.starter.ingest.testsupport;

import static com.google.common.collect.Lists.newArrayList;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.springframework.core.io.ClassPathResource;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.ingest.MessageWriter;
import com.timreardon.accumulo.starter.ingest.parser.MessageParser;

/**
 * Helper class that ingests data for use by other tests.
 */
public class IngestTestSupport {
    public static final String DATA_FILE_1 = "samples/maildir/allen-p/_sent_mail/1.";
    public static final String DATA_FILE_2 = "samples/maildir/mcconnell-m/all_documents/520.";
    public static final String DATA_FILE_3 = "samples/maildir/skilling-j/inbox/1.";
    public static final String DATA_FILE_4 = "samples/maildir/skilling-j/inbox/genie/1.";
    public static final String DATA_FILE_5 = "samples/maildir/skilling-j/sent/83.";
    public static final String DATA_FILE_6 = "samples/maildir/slinger-r/inbox/28.";
    
    protected static final String TABLE_NAME = "table";
    protected static final String INDEX_TABLE_NAME = "index";
    protected static final String[] TABLES = {TABLE_NAME, INDEX_TABLE_NAME};
    
    protected Connector connector;
    
    protected byte[] readDataFile(String path) throws IOException {
        return IOUtils.toByteArray(new ClassPathResource(path).getInputStream());
    }
    
    @Before
    public void setup() throws Exception {
        connector = new MockInstance("instance").getConnector("user", "");
        
        byte[] bytes = readDataFile(DATA_FILE_1);
        Message message = new MessageParser().parse(bytes, DATA_FILE_1);
        
        MessageWriter writer = new MessageWriter(connector, TABLE_NAME, INDEX_TABLE_NAME);
        writer.write(newArrayList(message));
        writer.close();
    }
    
    @After
    public void teardown() {
        for (String t : TABLES) {
            try {
                connector.tableOperations().delete(t);
            } catch (Exception ignored) {}
        }
    }
    
    protected void dumpTables() throws Exception {
        for (String table : TABLES) {
            System.out.println("Table: " + table);
            Scanner s = connector.createScanner(table, new Authorizations());
            Range r = new Range();
            s.setRange(r);
            for (Entry<Key,Value> entry : s) {
                System.out.println(entry.getKey().toString() + " " + entry.getValue().toString());
            }
        }
    }
}
