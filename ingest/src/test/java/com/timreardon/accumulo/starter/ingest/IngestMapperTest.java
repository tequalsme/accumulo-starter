package com.timreardon.accumulo.starter.ingest;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;

import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@RunWith(value = Parameterized.class)
public class IngestMapperTest {
    private static final String DATA_FILE_1 = "samples/maildir/allen-p/_sent_mail/1.";
    private static final String DATA_FILE_2 = "samples/maildir/mcconnell-m/all_documents/520.";
    private static final String DATA_FILE_3 = "samples/maildir/skilling-j/inbox/1.";
    private static final String DATA_FILE_4 = "samples/maildir/skilling-j/inbox/genie/1.";
    private static final String DATA_FILE_5 = "samples/maildir/skilling-j/sent/83.";
    
    @SuppressWarnings("rawtypes")
    private Context context;
    private Counter counter;
    private IngestMapper mapper;
    
    private String testDataPath;
    
    public IngestMapperTest(String testDataPath) {
        this.testDataPath = testDataPath;
    }

    @Parameters
    public static Collection<Object[]> data() {
      Object[][] data = new Object[][] { { DATA_FILE_1 }, { DATA_FILE_2 }, { DATA_FILE_3 }, { DATA_FILE_4 }, { DATA_FILE_5 } };
      return Arrays.asList(data);
    }
    
    @Before
    @SuppressWarnings("unchecked")
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.set(IngestConfiguration.TABLENAME, "data");
        conf.set(IngestConfiguration.INDEX_TABLENAME, "index");
        
        context = mock(Context.class);
        when(context.getConfiguration()).thenReturn(conf);
        counter = mock(Counter.class);
        when(context.getCounter("ingest", "count")).thenReturn(counter);

        mapper = new IngestMapper();
        mapper.setup(context);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMap() throws Exception {
        Resource r = new ClassPathResource(testDataPath);
        byte[] b = IOUtils.toByteArray(r.getInputStream());
        mapper.map(new Text(testDataPath), new BytesWritable(b), context);

        // TODO verify the mutations
        verify(context).write(eq(new Text("data")), any(Mutation.class));
        verify(context, atLeastOnce()).write(eq(new Text("index")), any(Mutation.class));
        
        verify(counter).increment(1);
    }
}
