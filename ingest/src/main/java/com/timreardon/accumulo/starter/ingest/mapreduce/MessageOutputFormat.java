package com.timreardon.accumulo.starter.ingest.mapreduce;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.ingest.MessageWriter;

/**
 * This class allows MapReduce jobs to easily write Messages to Accumulo. This output format accepts keys and values of
 * type NullWritable and Message from the Map() and Reduce() functions.
 * 
 * The user must specify the following via static methods:
 * 
 * <ul>
 * <li>{@link MessageOutputFormat#setOutputInfo(Configuration, String, byte[], String)}
 * <li>{@link MessageOutputFormat#setZooKeeperInstance(Configuration, String, String)} OR
 * {@link MessageOutputFormat#setMockInstance(Configuration, String)}
 * <li>{@link MessageOutputFormat#setTables(Configuration, String, String)}
 * </ul>
 * 
 * Other static methods are optional.
 */
public class MessageOutputFormat extends OutputFormat<NullWritable, Message> {
    private static final Logger logger = LoggerFactory.getLogger(MessageOutputFormat.class);

    private static final String PREFIX = MessageOutputFormat.class.getSimpleName();
    private static final String OUTPUT_INFO_SET = PREFIX + ".configured";
    private static final String INSTANCE_SET = PREFIX + ".instanceConfigured";
    private static final String TABLES_SET = PREFIX + ".instanceConfigured";

    private static final String USERNAME = PREFIX + ".username";
    private static final String PASSWORD = PREFIX + ".password";

    private static final String INSTANCE_NAME = PREFIX + ".instanceName";
    private static final String ZOOKEEPERS = PREFIX + ".zooKeepers";
    private static final String MOCK = ".useMockInstance";

    private static final String TABLE_NAME = PREFIX + ".tableName";
    private static final String INDEX_TABLE_NAME = PREFIX + ".indexTableName";

    private static final String BUFFER_SIZE = PREFIX + ".bufferSize";
    private static final int DEFAULT_BUFFER_SIZE = 500;

    // BatchWriter options
    private static final String MAX_MUTATION_BUFFER_SIZE = PREFIX + ".maxmemory";
    private static final String MAX_LATENCY = PREFIX + ".maxlatency";
    private static final String NUM_WRITE_THREADS = PREFIX + ".writethreads";

    private static final long DEFAULT_MAX_MUTATION_BUFFER_SIZE = MessageWriter.DEFAULT_MAX_MUTATION_BUFFER_SIZE;
    private static final int DEFAULT_MAX_LATENCY = MessageWriter.DEFAULT_MAX_LATENCY;
    private static final int DEFAULT_NUM_WRITE_THREADS = MessageWriter.DEFAULT_NUM_WRITE_THREADS;

    /**
     * Configure the output format.
     * 
     * @param conf
     * @param username
     *            Accumulo username
     * @param password
     *            Accumulo password
     */
    public static void setOutputInfo(Configuration conf, String username, byte[] password) {
        conf.set(USERNAME, username);
        conf.set(PASSWORD, new String(Base64.encodeBase64(password)));
        conf.setBoolean(OUTPUT_INFO_SET, true);
    }

    /**
     * Configure the zookeeper & accumulo instance information on the job.
     * 
     * @param conf
     * @param instance
     * @param zookeepers
     */
    public static void setZooKeeperInstance(Configuration conf, String instance, String zookeepers) {
        conf.set(INSTANCE_NAME, instance);
        conf.set(ZOOKEEPERS, zookeepers);
        conf.setBoolean(INSTANCE_SET, true);
    }

    /**
     * Configure the job to use a {@link MockInstance}.
     * 
     * @param conf
     * @param instanceName
     */
    public static void setMockInstance(Configuration conf, String instanceName) {
        conf.setBoolean(MOCK, true);
        conf.set(INSTANCE_NAME, instanceName);
        conf.setBoolean(INSTANCE_SET, true);
    }

    /**
     * Configure the table names.
     * 
     * @param conf
     * @param tableName
     * @param indexTableName
     */
    public static void setTables(Configuration conf, String tableName, String indexTableName) {
        conf.set(TABLE_NAME, tableName);
        conf.set(INDEX_TABLE_NAME, indexTableName);
        conf.set(TABLES_SET, indexTableName);
    }

    /**
     * Overrides the default streaming ingest buffer size.
     * 
     * @param conf
     * @param bufferSize
     */
    public static void setBufferSize(Configuration conf, int bufferSize) {
        conf.setInt(BUFFER_SIZE, bufferSize);
    }

    public static void setMaxMutationBufferSize(Configuration conf, long numberOfBytes) {
        conf.setLong(MAX_MUTATION_BUFFER_SIZE, numberOfBytes);
    }

    public static void setMaxLatency(Configuration conf, int numberOfMilliseconds) {
        conf.setInt(MAX_LATENCY, numberOfMilliseconds);
    }

    public static void setMaxWriteThreads(Configuration conf, int numberOfThreads) {
        conf.setInt(NUM_WRITE_THREADS, numberOfThreads);
    }

    /**
     * Builds a new accumulo instance based on info that was set on the job.
     * 
     * @param job
     * @return {@link Instance}
     */
    protected Instance getInstance(Configuration conf) {
        return (conf.getBoolean(MOCK, false) ? new MockInstance(conf.get(INSTANCE_NAME)) : new ZooKeeperInstance(
                conf.get(INSTANCE_NAME), conf.get(ZOOKEEPERS)));
    }

    /**
     * Returns the accumulo username that was set on the job.
     * 
     * @param job
     * @return username
     */
    protected String getUsername(Configuration conf) {
        return conf.get(USERNAME);
    }

    /**
     * Returns the accumulo password (decoded from base64) that was set on the job.
     * 
     * @param job
     * @return base64 decoded password
     */
    protected byte[] getPassword(Configuration conf) {
        return Base64.decodeBase64(conf.get(PASSWORD).getBytes());
    }

    /**
     * Returns the buffer size that was set on the job.
     * 
     * @param conf
     * @return buffer size
     */
    protected int getBufferSize(Configuration conf) {
        return conf.getInt(BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Returns the main table name that was set on the job.
     * 
     * @param conf
     * @return tableName
     */
    protected String getTableName(Configuration conf) {
        return conf.get(TABLE_NAME);
    }

    /**
     * Returns the index table name that was set on the job.
     * 
     * @param conf
     * @return indexTableName
     */
    protected String getIndexTableName(Configuration conf) {
        return conf.get(INDEX_TABLE_NAME);
    }

    protected long getMaxMutationBufferSize(Configuration conf) {
        return conf.getLong(MAX_MUTATION_BUFFER_SIZE, DEFAULT_MAX_MUTATION_BUFFER_SIZE);
    }

    protected int getMaxLatency(Configuration conf) {
        return conf.getInt(MAX_LATENCY, DEFAULT_MAX_LATENCY);
    }

    protected int getMaxWriteThreads(Configuration conf) {
        return conf.getInt(NUM_WRITE_THREADS, DEFAULT_NUM_WRITE_THREADS);
    }

    /**
     * Builds the MessageWriter.
     * 
     * @param conf
     * @return {@link MessageWriter}
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableExistsException
     * @throws TableNotFoundException
     */
    public MessageWriter getWriter(Configuration conf) throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException, TableExistsException {
        Instance instance = getInstance(conf);
        Connector connector = instance.getConnector(getUsername(conf), getPassword(conf));

        return new MessageWriter(connector, getTableName(conf), getIndexTableName(conf),
                getMaxMutationBufferSize(conf), getMaxLatency(conf), getMaxWriteThreads(conf));
    }

    /**
     * Builds a new MessageRecordWriter.
     * 
     * @param attempt
     * @return {@link RecordWriter}
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<NullWritable, Message> getRecordWriter(TaskAttemptContext attempt) throws IOException,
            InterruptedException {
        try {
            return new MessageRecordWriter(attempt);
        } catch (Exception e) {
            throw new IOException("Error returning new MessageRecordWriter", e);
        }
    }

    /**
     * Verifies that correct inputs are set so that the job can fail gracefully.
     * 
     * @param job
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();

        if (!conf.getBoolean(OUTPUT_INFO_SET, false)) {
            throw new IOException("Output info has not been set.");
        }

        if (!conf.getBoolean(INSTANCE_SET, false)) {
            throw new IOException("Instance info has not been set.");
        }

        if (!conf.getBoolean(TABLES_SET, false)) {
            throw new IOException("Table names have not been set.");
        }

        // verify connection
        try {
            Connector c = getInstance(conf).getConnector(getUsername(conf), getPassword(conf));
            if (!c.securityOperations().authenticateUser(getUsername(conf), getPassword(conf))) {
                throw new IOException("Unable to authenticate accumulo user.");
            }
        } catch (AccumuloSecurityException e) {
            throw new IOException(e);
        } catch (AccumuloException e) {
            throw new IOException(e);
        }
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext attempt) throws IOException, InterruptedException {
        return new NullOutputFormat<Text, Message>().getOutputCommitter(attempt);
    }

    /**
     * Class for writing messages to the underlying {@link MessageWriter} via a streamable output buffer.
     */
    protected class MessageRecordWriter extends RecordWriter<NullWritable, Message> {
        private final MessageWriter writer;
        private final int bufferSize;
        private final BlockingQueue<Message> buffer;

        private long count = 0;

        MessageRecordWriter(JobContext job) throws Exception {
            Configuration conf = job.getConfiguration();
            this.writer = getWriter(conf);
            this.bufferSize = getBufferSize(conf);
            this.buffer = new ArrayBlockingQueue<Message>(bufferSize);
        }

        /**
         * Write method adds a message to the buffer (and dumps the buffer once it's full).
         * 
         * @param text
         * @param message
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(NullWritable nullWritable, Message message) throws IOException, InterruptedException {
            count++;
            if (!buffer.offer(message)) {
                dumpBufferAndWrite();
                buffer.add(message);
            }
        }

        /**
         * Dumps any messages left in the buffer.
         * 
         * @param context
         * @throws IOException
         */
        @Override
        public void close(TaskAttemptContext attempt) throws IOException {
            if (buffer.size() > 0) {
                dumpBufferAndWrite();
            }

            logger.info("Messages written: " + count);
        }

        private void dumpBufferAndWrite() throws IOException {
            try {
                writer.write(buffer);
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("Wrote %d messages", buffer.size()));
                }
                buffer.clear();
            } catch (Exception e) {
                throw new IOException("An error occurred storing messages. [buffer=<" + buffer + ">]", e);
            }
        }
    }
}
