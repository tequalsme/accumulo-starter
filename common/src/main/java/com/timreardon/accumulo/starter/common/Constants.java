package com.timreardon.accumulo.starter.common;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public final class Constants {

    private Constants() {
        // empty
    }
    
    public static final String FIELD_COLUMN_FAMILY = "f";
    public static final Text DOCUMENT_COLUMN_FAMILY = new Text("d");
    
    public static final String NULL_BYTE = "\u0000";
    public static final String MAX_BYTE = "\uffff";
    
    public static final Text EMPTY_TEXT = new Text();
    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final Value EMPTY_VALUE = new Value(EMPTY_BYTES);
    
    public static final ColumnVisibility EMPTY_COL_VIS = new ColumnVisibility();
}
