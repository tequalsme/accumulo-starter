package com.timreardon.accumulo.starter.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
        
        Job job = new Job(getConf(), getClass().getSimpleName());
        Configuration conf = job.getConfiguration();
        conf.set("mapred.map.tasks.speculative.execution", "false");

        String zookeepers = IngestConfiguration.getZookeepers(conf);
        String instanceName = IngestConfiguration.getInstanceName(conf);
        String user = IngestConfiguration.getUsername(conf);
        byte[] password = IngestConfiguration.getPassword(conf);
        String tableName = IngestConfiguration.getTableName(conf);
        
        Connector connector = new ZooKeeperInstance(instanceName, zookeepers).getConnector(user, password);
        createTables(connector, conf);

        job.setJarByClass(IngestJob.class);
        job.setInputFormatClass(WholeFileInputFormat.class);

        Path inputPath = new Path(args[0]);
        FileSystem fs = FileSystem.get(conf);
        List<Path> inputPaths = new ArrayList<Path>();
        listFiles(inputPath, fs, inputPaths);
        
        System.out.println("Input files in " + inputPath + ":" + inputPaths.size());
        Path[] inputPathsArray = new Path[inputPaths.size()];
        inputPaths.toArray(inputPathsArray);
        
        String s = inputPathsArray[0].toString();
        int i = s.indexOf("maildir/");
//        int i2 = s.indexOf('/', i+8);
//        String mailbox = s.substring(i+8, i2);
//        String folder = s.substring(i2+1);
        System.out.println("path: " + s);
        String[] folderTokens = s.substring(i+8).split("/");
        System.out.println("mailbox: " + folderTokens[0]);
        System.out.println("folder: " + folderTokens[1]);
        System.out.println("file: " + folderTokens[folderTokens.length-1]);
        
//        FileInputFormat.setInputPaths(job, inputPathsArray);
//
//        job.setMapperClass(IngestMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Mutation.class);
//        job.setNumReduceTasks(0);
//        job.setOutputFormatClass(AccumuloOutputFormat.class);
//        AccumuloOutputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);
//        AccumuloOutputFormat.setOutputInfo(conf, user, password, true, tableName);
//
//        return job.waitForCompletion(true) ? 0 : 1;
        return 0;
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
    
    private void createTables(Connector connector, Configuration conf) throws AccumuloException, AccumuloSecurityException {
        TableOperations tops = connector.tableOperations();

        try {
            if (!tops.exists(IngestConfiguration.getTableName(conf))) {
                tops.create(IngestConfiguration.getTableName(conf));
            }

            if (!tops.exists(IngestConfiguration.getIndexTableName(conf))) {
                tops.create(IngestConfiguration.getIndexTableName(conf));
            }
        } catch (TableExistsException e) {
            // shouldn't happen as we check for table existence prior to each create() call
            throw new AccumuloException(e);
        }
    }
}
