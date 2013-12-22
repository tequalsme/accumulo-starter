package com.timreardon.accumulo.starter.query.impl;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.ingest.testsupport.IngestTestSupport;

public class QueryServiceImplTest extends IngestTestSupport {
    private QueryServiceImpl service;
    
    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        
        dumpTables();
        
        service = new QueryServiceImpl(connector, TABLE_NAME, INDEX_TABLE_NAME);
    }
    
    @Test
    public void testQuery() {
        List<Message> messages = service.query("forecast");
        assertEquals(1, messages.size());
    }
    
    @Test
    public void testQueryNoMatch() {
        List<Message> messages = service.query("blah");
        assertEquals(0, messages.size());
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testQueryNullTerm() {
        service.query(null);
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testQueryEmptyTerm() {
        service.query("");
    }
}
