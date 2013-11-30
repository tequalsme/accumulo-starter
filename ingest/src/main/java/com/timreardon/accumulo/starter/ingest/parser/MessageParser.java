package com.timreardon.accumulo.starter.ingest.parser;

import static java.util.Arrays.asList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class MessageParser {
    private static final Logger logger = LoggerFactory.getLogger(MessageParser.class);
    
    private static final String ID = "Message-ID";
    private static final String DATE = "Date";
    private static final String FROM = "From";
    private static final String TO = "To";
    private static final String CC = "Cc";
    private static final String BCC = "Bcc";
    private static final String SUBJECT = "Subject";
    
    SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");

    public Message parse(byte[] bytes, String path) throws IOException {
        InputStream in = new ByteArrayInputStream(bytes);
        
        @SuppressWarnings("unchecked")
        List<String> lines = IOUtils.readLines(in);
        
        Message message = new Message();
        //Map<String, String> headers = new HashMap<String, String>();
        Multimap<String, String> headers = HashMultimap.create();
        Set<String> bodyTokens = new HashSet<String>();
        boolean inHeaders = true;
        String headerKey = null, headerLine = null;
        
        for (String line : lines) {
            // an empty line delineates headers and body
            if (line.isEmpty()) {
                inHeaders = false;
                continue;
            }
            
            if (!inHeaders) {
                processBody(line, bodyTokens);
                continue;
            }
            
            // else process headers:
            headerLine = null;
            if (line.contains(": ")) {
                String[] h = line.split(": ", 2);
                headerKey = h[0];
                headerLine = h[1];
                
            } else if (line.startsWith("\u0009" /* tab */) || line.startsWith(" ")) {
                // continuation of previous header line
                headerLine = line.trim();
            }
            
            if (StringUtils.isEmpty(headerLine)) {
                continue;
            }
            
            if (ID.equals(headerKey)) {
                message.setId(headerLine);
            } else if (DATE.equals(headerKey)) {
                try {
                    message.setTimestamp(sdf.parse(headerLine).getTime());
                } catch (ParseException e) {
                    logger.warn("Unable to parse message date", e);
                }
            } else if (FROM.equals(headerKey)) {
                message.setFrom(headerLine);
            } else if (TO.equals(headerKey)) {
//                message.addTo(headerLine);
                String[] headerValues = (headerLine.contains(",") ? headerLine.split(",") : new String[] {headerLine});
                for (String val : headerValues) {
                    message.addTo(val.trim());
                }
            } else if (CC.equals(headerKey)) {
//                message.addCc(headerLine);
                String[] headerValues = (headerLine.contains(",") ? headerLine.split(",") : new String[] {headerLine});
                for (String val : headerValues) {
                    message.addCc(val.trim());
                }
            } else if (BCC.equals(headerKey)) {
//                message.addBcc(headerLine);
                String[] headerValues = (headerLine.contains(",") ? headerLine.split(",") : new String[] {headerLine});
                for (String val : headerValues) {
                    message.addBcc(val.trim());
                }
            } else if (SUBJECT.equals(headerKey)) {
                message.setSubject(headerLine);
            } else {
                String[] headerValues = (headerLine.contains(",") ? headerLine.split(",") : new String[] {headerLine});
                for (String val : headerValues) {
                    headers.put(headerKey, val.trim());
                }
            }
            
//            if (inHeaders && line.contains(": ") && !line.isEmpty()) {
//                processHeaders(line, message, headers);
//            } else {
//                inHeaders = false;
//                if (line.isEmpty()) {
//                    continue;
//                } else {
//                    processBody(line, words);
//                }
//            }
        }
        
//        message.setHeaders(headers);
        message.setBodyTokens(bodyTokens);
        message.setRawBytes(bytes);
        
        parsePath(path, message);
        
        return message;
    }
    
//    private void parseField() {
//        String[] headerValues = (headerLine.contains(",") ? headerLine.split(",") : new String[] {headerLine});
//        for (String val : headerValues) {
//            headers.put(headerKey, val.trim());
//        }
//    }
    
    private void parsePath(String path, Message message) {
        int i = path.indexOf("maildir/");
        if (i != -1) {
            String[] folderTokens = path.substring(i+8).split("/");
            message.setMailbox(folderTokens[0]);
            message.setFolder(folderTokens[1]);
            message.setFileName(folderTokens[folderTokens.length-1]);
        }
    }

//    private void processHeaders(String line, Message message, Map<String, String> headers) {
////        System.out.println(line);
//        String[] h = line.split(": ", 2);
//        if (h.length >= 2) {
//            if (StringUtils.isEmpty(h[1])) {
//                return;
//            }
//            
//            if (ID.equals(h[0]))
//                message.setId(h[1]);
//            else if (DATE.equals(h[0])) {
//                try {
//                    message.setTimestamp(sdf.parse(h[1]).getTime());
//                } catch (ParseException e) {
//                    logger.warn("Unable to parse message date", e);
//                }
//            } else if (FROM.equals(h[0]))
//                message.setFrom(h[1]);
//            else if (TO.equals(h[0]))
//                message.setTo(h[1]);
//            else if (CC.equals(h[0]))
//                message.setCc(h[1]);
//            else if (BCC.equals(h[0]))
//                message.setBcc(h[1]);
//            else if (SUBJECT.equals(h[0]))
//                message.setSubject(h[1]);
//            else
//                headers.put(h[0], h[1]);
//        }
//    }
    
    private void processBody(String line, Set<String> words) {
        // lowercase, split on non-alphanumeric chars
        words.addAll(asList(line.toLowerCase().split("\\W+")));
    }
}
