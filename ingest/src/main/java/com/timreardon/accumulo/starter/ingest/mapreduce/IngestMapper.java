package com.timreardon.accumulo.starter.ingest.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.ingest.parser.MessageParser;

public class IngestMapper extends Mapper<Text, BytesWritable, NullWritable, Message> {
    private MessageParser parser;

    private static final NullWritable NULL = NullWritable.get();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        parser = new MessageParser();
    }

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Message message = parser.parse(value.getBytes(), key.toString());
        
        context.write(NULL, message);
    }
}
