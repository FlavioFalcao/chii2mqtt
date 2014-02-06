package org.chii2.mqtt.common.message;

/**
 * PUBCOMP Message - Assured Publish Complete (Part 3)
 * <p/>
 * This message is either the response from the server to a PUBREL message from a
 * publisher, or the response from a subscriber to a PUBREL message from the server. It
 * is the fourth and last message in the QoS 2 protocol flow.
 */
public class PubCompMessage extends MQTTMessage {

    // Message ID
    protected int messageID;

    /**
     * INTERNAL USE ONLY
     */
    public PubCompMessage() {
        // Set Message Type
        this.messageType = MessageType.PUBCOMP;
    }

    public PubCompMessage(int messageID) {
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
