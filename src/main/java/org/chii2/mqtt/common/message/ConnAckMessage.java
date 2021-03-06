package org.chii2.mqtt.common.message;

/**
 * CONNACK Message - Acknowledge connection request
 * <p/>
 * The CONNACK message is the message sent by the server in response to a CONNECT
 * request from a client.
 */
public class ConnAckMessage extends MQTTMessage {

    // Return Code
    public static enum ReturnCode {
        CONNECTION_ACCEPTED(0),             // Accepted
        UNACCEPTABLE_PROTOCOL_VERSION(1),   // Refused
        IDENTIFIER_REJECTED(2),             // Refused
        SERVER_UNAVAILABLE(3),              // Refused
        BAD_USERNAME_OR_PASSWORD(4),        // Refused
        NOT_AUTHORIZED(5);                  // Refused

        private byte value;

        ReturnCode(int value) {
            this.value = (byte) value;
        }

        public byte getReturnCodeValue() {
            return value;
        }
    }

    // Return Code
    protected ReturnCode returnCode;

    /**
     * INTERNAL USE ONLY
     */
    public ConnAckMessage() {
        // Set Message Type
        this.messageType = MessageType.CONNACK;
    }

    public ConnAckMessage(ReturnCode returnCode) {
        this();
        this.returnCode = returnCode;
        this.remainingLength = calculateRemainingLength();
    }

    @Override
    protected int calculateRemainingLength() {
        return 2;
    }

    public ReturnCode getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(ReturnCode returnCode) {
        this.returnCode = returnCode;
    }
}
