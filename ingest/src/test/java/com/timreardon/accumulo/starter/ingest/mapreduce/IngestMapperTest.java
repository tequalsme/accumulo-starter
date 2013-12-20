package com.timreardon.accumulo.starter.ingest.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.timreardon.accumulo.starter.common.domain.Message;

public class IngestMapperTest {
    private static final String DATA_FILE_1 = "samples/maildir/allen-p/_sent_mail/1.";
    
    @SuppressWarnings("rawtypes")
    private Context context;
    
    private IngestMapper mapper;
    
    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set(IngestConfiguration.TABLENAME, "data");
        conf.set(IngestConfiguration.INDEX_TABLENAME, "index");
        
        context = mock(Context.class);
        when(context.getConfiguration()).thenReturn(conf);

        mapper = new IngestMapper();
        mapper.setup(context);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMap() throws Exception {
        Resource r = new ClassPathResource(DATA_FILE_1);
        byte[] b = IOUtils.toByteArray(r.getInputStream());
        mapper.map(new Text(DATA_FILE_1), new BytesWritable(b), context);

        ArgumentCaptor<Message> arg = ArgumentCaptor.forClass(Message.class);
        verify(context).write(eq(NullWritable.get()), arg.capture());
        
        Message message = arg.getValue();
        assertEquals("18782981.1075855378110", message.getId());
    }
}
