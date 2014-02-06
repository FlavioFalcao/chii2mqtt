package org.chii2.mqtt.common.message;

/**
 * UNSUBACK Message - Unsubscribe Acknowledgment
 * <p/>
 * The UNSUBACK message is sent by the server to the client to confirm receipt of an
 * UNSUBSCRIBE message.
 */
public class UnsubAckMessage extends MQTTMessage {

    // Message ID
    protected int messageID;

    /**
     * INTERNAL USE ONLY
     */
    public UnsubAckMessage() {
        // Set Message Type
        this.messageType = MessageType.UNSUBACK;
    }

    public UnsubAckMessage(int messageID) {
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
