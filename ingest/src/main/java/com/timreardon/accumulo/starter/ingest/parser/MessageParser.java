package com.timreardon.accumulo.starter.ingest.parser;

import static java.util.Arrays.asList;
import static org.apache.commons.io.IOUtils.readLines;
import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.remove;
import static org.apache.commons.lang.StringUtils.strip;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.timreardon.accumulo.starter.common.domain.Message;

/**
 * Custom logic for parsing Enron email messages.
 * 
 * @author tim
 */
public class MessageParser {
    private static final Logger logger = LoggerFactory.getLogger(MessageParser.class);
    
    private static final String ID = "Message-ID";
    private static final String DATE = "Date";
    private static final String FROM = "From";
    private static final String TO = "To";
    private static final String CC = "Cc";
    private static final String BCC = "Bcc";
    private static final String SUBJECT = "Subject";
    
    static final String DATE_PATTERN = "EEE, d MMM yyyy HH:mm:ss Z";

    public Message parse(byte[] bytes, String path) throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_PATTERN);
        
        InputStream in = new ByteArrayInputStream(bytes);
        
        @SuppressWarnings("unchecked")
        List<String> lines = readLines(in);
        
        Message message = new Message();
        
        parsePath(path, message);

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
            
            if (isEmpty(headerLine)) {
                continue;
            }
            
            if (ID.equals(headerKey)) {
                message.setId(strip(remove(headerLine, ".JavaMail.evans@thyme"), "<>"));
            } else if (DATE.equals(headerKey)) {
                try {
                    message.setTimestamp(sdf.parse(headerLine).getTime());
                } catch (ParseException e) {
                    logger.warn("Unable to parse message date", e);
                }
            } else if (FROM.equals(headerKey)) {
                message.setFrom(headerLine);
            } else if (TO.equals(headerKey)) {
                message.addTo(parseHeaderLine(headerLine));
            } else if (CC.equals(headerKey)) {
                message.addCc(parseHeaderLine(headerLine));
            } else if (BCC.equals(headerKey)) {
                message.addBcc(parseHeaderLine(headerLine));
            } else if (SUBJECT.equals(headerKey)) {
                message.setSubject(headerLine);
            } else {
                // misc headers
                String[] headerValues = parseHeaderLine(headerLine);
                for (String val : headerValues) {
                    headers.put(headerKey, val.trim());
                }
            }
        }
        
//        message.setHeaders(headers);
        message.setBodyTokens(bodyTokens);
        message.setRawBytes(bytes);
        
        return message;
    }
    
    private String[] parseHeaderLine(String headerLine) {
        return (headerLine.contains(",") ? headerLine.split(",") : new String[] {headerLine});
    }
    
    private void parsePath(String path, Message message) {
        int i = path.indexOf("maildir/");
        if (i != -1) {
            String[] folderTokens = path.substring(i+8).split("/");
            message.setMailbox(folderTokens[0]);
            message.setFolder(folderTokens[1]);
            message.setFileName(folderTokens[folderTokens.length-1]);
        }
    }
    
    private void processBody(String line, Set<String> words) {
        // lowercase, split on non-alphanumeric chars
        words.addAll(asList(line.toLowerCase().split("\\W+")));
    }
}
