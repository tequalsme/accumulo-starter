package com.timreardon.accumulo.starter.ingest.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Multimap;

public class Message {
    private String id, from, subject;
    private List<String> to, cc, bcc;
    private long timestamp;
    private String mailbox, folder, filename;
//    private Multimap<String, String> headers;
    private Set<String> bodyTokens;
    private byte[] rawBytes;
    
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getFrom() {
        return from;
    }
    public void setFrom(String from) {
        this.from = from;
    }
    public List<String> getTo() {
        return to;
    }
    public String getToAsString() {
        return StringUtils.join(to, ',');
    }
    public void addTo(String to) {
        if (this.to == null) {
            this.to = new ArrayList<String>();
        }
        if (StringUtils.isNotBlank(to))
            this.to.add(to);
    }
    public List<String> getCc() {
        return cc;
    }
    public String getCcAsString() {
        return StringUtils.join(cc, ',');
    }
    public void addCc(String cc) {
        if (this.cc == null) {
            this.cc = new ArrayList<String>();
        }
        if (StringUtils.isNotBlank(cc))
            this.cc.add(cc);
    }
    public List<String> getBcc() {
        return bcc;
    }
    public String getBccAsString() {
        return StringUtils.join(bcc, ',');
    }
    public void addBcc(String bcc) {
        if (this.bcc == null) {
            this.bcc = new ArrayList<String>();
        }
        if (StringUtils.isNotBlank(bcc))
            this.bcc.add(bcc);
    }
    public String getSubject() {
        return subject;
    }
    public void setSubject(String subject) {
        this.subject = subject;
    }
    public String getFilename() {
        return filename;
    }
    public void setFilename(String filename) {
        this.filename = filename;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public String getMailbox() {
        return mailbox;
    }
    public void setMailbox(String mailbox) {
        this.mailbox = mailbox;
    }
    public String getFolder() {
        return folder;
    }
    public void setFolder(String folder) {
        this.folder = folder;
    }
    public String getFileName() {
        return filename;
    }
    public void setFileName(String filename) {
        this.filename = filename;
    }
//    public Multimap<String, String> getHeaders() {
//        return headers;
//    }
//    public void setHeaders(Multimap<String, String> headers) {
//        this.headers = headers;
//    }
    public Set<String> getBodyTokens() {
        return bodyTokens;
    }
    public void setBodyTokens(Set<String> bodyTokens) {
        this.bodyTokens = bodyTokens;
    }
    public byte[] getRawBytes() {
        return rawBytes;
    }
    public void setRawBytes(byte[] rawBytes) {
        this.rawBytes = rawBytes;
    }
    
//    @Override
//    public String toString() {
//        StringBuilder builder = new StringBuilder();
//        builder.append("Message [id=").append(id).append(", timestamp=").append(timestamp).append(", mailbox=")
//                .append(mailbox).append(", folder=").append(folder).append(", filename=").append(filename)
//                .append(", headers=").append(headers).append(", bodyTokens=").append(bodyTokens).append(", rawBytes=")
//                .append("]");
//        return builder.toString();
//    }
}
