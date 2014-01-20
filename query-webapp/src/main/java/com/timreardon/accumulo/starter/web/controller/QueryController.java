package com.timreardon.accumulo.starter.web.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.timreardon.accumulo.starter.common.domain.Message;
import com.timreardon.accumulo.starter.query.QueryService;

@Controller
public class QueryController {

    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    @Autowired
    private QueryService queryService;

    @RequestMapping(value = "/query", method = RequestMethod.GET)
    @ResponseBody
    public List<Message> query(@RequestParam String term) {
		logger.debug("query: term=" + term);
        return queryService.query(term);
    }
}
