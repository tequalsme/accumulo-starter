package com.timreardon.accumulo.starter.ingest;

import org.apache.hadoop.conf.Configuration;

public class IngestConfiguration {
    private static final String PREFIX = IngestConfiguration.class.getSimpleName();

    public final static String INSTANCE = PREFIX + ".instance";
    public final static String ZOOKEEPERS = PREFIX + ".zookeepers";
    public final static String USERNAME = PREFIX + ".username";
    public final static String PASSWORD = PREFIX + ".password";
    
    public final static String TABLENAME = PREFIX + ".tableName";
    public final static String INDEX_TABLENAME = PREFIX + ".indexTableName";

    public static String getInstanceName(Configuration conf) {
        return conf.get(INSTANCE);
    }

    public static String getZookeepers(Configuration conf) {
        return conf.get(ZOOKEEPERS);
    }

    public static String getUsername(Configuration conf) {
        return conf.get(USERNAME);
    }

    public static byte[] getPassword(Configuration conf) {
        String pass = conf.get(PASSWORD);
        if (pass == null) {
            return null;
        }
        return pass.getBytes();
    }

    public static String getTableName(Configuration conf) {
        return conf.get(TABLENAME);
    }
    
    public static String getIndexTableName(Configuration conf) {
        return conf.get(INDEX_TABLENAME);
    }
}
