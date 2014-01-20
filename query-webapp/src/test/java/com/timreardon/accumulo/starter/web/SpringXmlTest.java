package com.timreardon.accumulo.starter.web;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Validate that Spring XML context files are properly parsed.
 * 
 * @author tim
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"/applicationContext.xml", "/testApplicationContext.xml"})
public class SpringXmlTest {
    @Test
    public void testSpringXmlContextLoad() {
        // empty
    }
}
