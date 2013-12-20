package com.timreardon.accumulo.starter.ingest.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.timreardon.accumulo.starter.common.domain.Message;

public class IngestJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IngestJob(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println(String.format("Usage: %s [datadir]", getClass().getName()));
            return 1;
        }
        
        Configuration conf = getConf();
        conf.set("mapred.map.tasks.speculative.execution", "false");

        String instanceName = IngestConfiguration.getInstanceName(conf);
        String zookeepers = IngestConfiguration.getZookeepers(conf);
        String user = IngestConfiguration.getUsername(conf);
        byte[] password = IngestConfiguration.getPassword(conf);
        String tableName = IngestConfiguration.getTableName(conf);
        String indexTableName = IngestConfiguration.getIndexTableName(conf);

        Path inputPath = new Path(args[0]);
        FileSystem fs = FileSystem.get(conf);
        List<Path> inputPaths = new ArrayList<Path>();
        listFiles(inputPath, fs, inputPaths);
        
        System.out.println("Input files in " + inputPath + ":" + inputPaths.size());
        Path[] inputPathsArray = new Path[inputPaths.size()];
        inputPaths.toArray(inputPathsArray);

        Job job = new Job(conf, getClass().getSimpleName());
        job.setJarByClass(IngestJob.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(MessageOutputFormat.class);

        job.setMapperClass(IngestMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Message.class);
        job.setNumReduceTasks(0);
        
        FileInputFormat.setInputPaths(job, inputPathsArray);
        
        MessageOutputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);
        MessageOutputFormat.setOutputInfo(conf, user, password);
        MessageOutputFormat.setTables(conf, tableName, indexTableName);

        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    /**
     * Recursively add all files under path to the given list.
     * 
     * @param path
     * @param fs
     * @param files
     * @throws IOException
     */
    protected void listFiles(Path path, FileSystem fs, List<Path> files) throws IOException {
      for (FileStatus status : fs.listStatus(path)) {
        if (status.isDir()) {
          listFiles(status.getPath(), fs, files);
        } else {
          files.add(status.getPath());
        }
      }
    }
}
