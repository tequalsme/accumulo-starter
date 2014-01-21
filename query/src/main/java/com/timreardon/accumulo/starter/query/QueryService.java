package com.timreardon.accumulo.starter.query;

import org.calrissian.mango.collect.CloseableIterable;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.query.impl.QueryException;

public interface QueryService {

    CloseableIterable<Message> query(String term) throws QueryException;
    
    // TODO: forthcoming
    // CloseableIterable<Message> query(String... term) throws QueryException;
}
