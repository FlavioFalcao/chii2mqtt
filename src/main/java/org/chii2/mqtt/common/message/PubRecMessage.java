package org.chii2.mqtt.common.message;

/**
 * PUBREC Message - Assured Publish Received (Part 1)
 * <p/>
 * A PUBREC message is the response to a PUBLISH message with QoS level 2. It is the
 * second message of the QoS level 2 protocol flow. A PUBREC message is sent by the
 * server in response to a PUBLISH message from a publishing client, or by a subscriber in
 * response to a PUBLISH message from the server.
 */
public class PubRecMessage extends MQTTMessage {

    // Message ID
    protected int messageID;

    /**
     * INTERNAL USE ONLY
     */
    public PubRecMessage() {
        // Set Message Type
        this.messageType = MessageType.PUBREC;
    }

    public PubRecMessage(int messageID) {
        this();
        this.messageID = messageID;
        this.remainingLength = calculateRemainingLength();
    }

    @Override
    protected int calculateRemainingLength() {
        return 2;
    }

    public int getMessageID() {
        return messageID;
    }

    public void setMessageID(int messageID) {
        this.messageID = messageID;
    }
}
