package com.timreardon.accumulo.starter.common.domain;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Message {
    private static final char DELIMITER = ',';
    
    private String id, from, subject;
    private List<String> to, cc, bcc;
    private long timestamp;
    private String mailbox, folder, filename;
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
        return join(to, DELIMITER);
    }

    public void addTo(String... values) {
        if (this.to == null) {
            this.to = new ArrayList<String>();
        }

        for (String v : values) {
            if (isNotBlank(v)) {
                this.to.add(v.trim());
            }
        }
    }

    public List<String> getCc() {
        return cc;
    }

    public String getCcAsString() {
        return join(cc, DELIMITER);
    }

    public void addCc(String... values) {
        if (this.cc == null) {
            this.cc = new ArrayList<String>();
        }
        
        for (String v : values) {
            if (isNotBlank(v)) {
                this.cc.add(v.trim());
            }
        }
    }

    public List<String> getBcc() {
        return bcc;
    }

    public String getBccAsString() {
        return join(bcc, DELIMITER);
    }

    public void addBcc(String... values) {
        if (this.bcc == null) {
            this.bcc = new ArrayList<String>();
        }
        
        for (String v : values) {
            if (isNotBlank(v)) {
                this.bcc.add(v.trim());
            }
        }
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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Message [id=").append(id).append(", from=").append(from).append(", subject=").append(subject)
                .append(", to=").append(to).append(", timestamp=").append(timestamp).append("]");
        return builder.toString();
    }
}
