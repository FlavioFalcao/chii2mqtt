package org.chii2.mqtt.server.storage;

import org.chii2.mqtt.common.message.PublishMessage;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * MQTT PUBLISH Meesgae in Storage
 */
public class StoredPublishMessage implements Serializable {

    private String publisherID;
    private int qosLevel;
    private String topicName;
    private int messageID;
    private byte[] content;

    public StoredPublishMessage(String clientID, PublishMessage publishMessage) {
        this.publisherID = clientID;
        this.qosLevel = publishMessage.getQosLevel().getQoSValue();
        this.topicName = publishMessage.getTopicName();
        this.messageID = publishMessage.getMessageID();
        ByteBuffer buffer = publishMessage.getContent();
        buffer.rewind();
        content = new byte[buffer.limit()];
        buffer.get(content);
        buffer.rewind();
    }

    /**
     * This create a FAKE StorePublishMessage
     * It should only be used to check storage whether contains specific MessageID
     *
     * @param clientID Publisher ID
     * @param messageID PUBLISH MessageID
     */
    public StoredPublishMessage(String clientID, int messageID) {
        this.publisherID = clientID;
        this.messageID = messageID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoredPublishMessage that = (StoredPublishMessage) o;

        if (messageID != that.messageID) return false;
        if (!publisherID.equals(that.publisherID)) return false;

        return true;
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

    public int getQosLevel() {
        return qosLevel;
    }

    public void setQosLevel(int qosLevel) {
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
