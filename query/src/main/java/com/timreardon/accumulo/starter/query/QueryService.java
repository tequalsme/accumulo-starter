package com.timreardon.accumulo.starter.query;

import java.util.List;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.query.impl.QueryException;

public interface QueryService {

    // term search
    // TODO create a better query API
    List<Message> query(String term) throws QueryException;
}
