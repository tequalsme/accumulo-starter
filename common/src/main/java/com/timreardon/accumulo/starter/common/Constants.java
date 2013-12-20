package com.timreardon.accumulo.starter.common;

import static org.apache.commons.lang.ArrayUtils.EMPTY_BYTE_ARRAY;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public final class Constants {

    private Constants() {
        // empty
    }
    
    public static final String FIELD_COLUMN_FAMILY = "f";
    public static final Text DOCUMENT_COLUMN_FAMILY = new Text("d");

    /** "\u0000" */
    public static final String MIN_CHAR_STRING = Character.toString(Character.MIN_VALUE);
    /** "\uffff" */
    public static final String MAX_CHAR_STRING = Character.toString(Character.MAX_VALUE);

    public static final Text EMPTY_TEXT = new Text();
    public static final Value EMPTY_VALUE = new Value(EMPTY_BYTE_ARRAY);
    public static final ColumnVisibility EMPTY_COL_VIS = new ColumnVisibility();
}
