package org.chii2.mqtt.common.message;

import java.nio.ByteBuffer;

/**
 * PUBLISH Message - Publish Message
 * <p/>
 * A PUBLISH message is sent by a client to a server for distribution to interested
 * subscribers. Each PUBLISH message is associated with a topic name (also known as the
 * Subject or Channel). This is a hierarchical name space that defines a taxonomy of
 * information sources for which subscribers can register an interest. A message that is
 * published to a specific topic name is delivered to connected subscribers for that topic.
 * <p/>
 * If a client subscribes to one or more topics, any message published to those topics are
 * sent by the server to the client as a PUBLISH message.
 */
public class PublishMessage extends MQTTMessage {

    // Topic Name
    protected String topicName;
    // Message ID, Not present if Qos level is 0
    protected int messageID;
    // Payload
    protected ByteBuffer content;

    /**
     * INTERNAL USE ONLY
     */
    public PublishMessage() {
        // Set Message Type
        this.messageType = MessageType.PUBLISH;
    }

    public PublishMessage(boolean retain, QoSLevel qosLevel, boolean dupFlag, String topicName, int messageID, ByteBuffer content) {
        this();
        this.retain = retain;
        this.qosLevel = qosLevel;
        this.dupFlag = dupFlag;
        this.topicName = topicName;
        this.messageID = messageID;
        setContent(content);
        this.remainingLength = calculateRemainingLength();
    }

    @Override
    protected int calculateRemainingLength() {
        int length = 0;
        length = length + 2 + topicName.getBytes().length;
        length = length + 2;
        length = length + content.limit();
        return length;
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

    public ByteBuffer getContent() {
        return content;
    }

    /**
     * Set Content
     * Make sure the ByteBuffer's position and limit is correct.
     * If the position is not 0, it will be flipped.
     *
     * @param content ByteBuffer Content
     */
    public void setContent(ByteBuffer content) {
        if (content.position() != 0) {
            content.flip();
        }
        this.content = content;
    }
}
