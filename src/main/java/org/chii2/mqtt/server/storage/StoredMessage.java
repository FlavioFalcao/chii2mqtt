package org.chii2.mqtt.server.storage;

import java.io.Serializable;

/**
 * Stored Message
 */
public class StoredMessage implements Serializable {

    private String publisherID;
    private boolean retain;
    private int qosLevel;
    private String topicName;
    private int messageID;
    private byte[] content;

    public StoredMessage(String publisherID, boolean retain, int qosLevel, String topicName, int messageID, byte[] content) {
        this.publisherID = publisherID;
        this.retain = retain;
        this.qosLevel = qosLevel;
        this.topicName = topicName;
        this.messageID = messageID;
        this.content = content;
    }

    public StoredMessage(String publisherID, int messageID) {
        this.publisherID = publisherID;
        this.messageID = messageID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoredMessage that = (StoredMessage) o;

        return messageID == that.messageID && publisherID.equals(that.publisherID);

    }

    @Override
    public int hashCode() {
        int result = publisherID.hashCode();
        result = 31 * result + messageID;
        return result;
    }

    public String getPublisherID() {
        return publisherID;
    }

    public void setPublisherID(String publisherID) {
        this.publisherID = publisherID;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public int getQoSLevel() {
        return qosLevel;
    }

    public void setQoSLevel(int qosLevel) {
        this.qosLevel = qosLevel;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getMessageID() {
        return messageID;
    }

    public void setMessageID(int messageID) {
        this.messageID = messageID;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }
}
