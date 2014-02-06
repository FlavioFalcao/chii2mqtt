package org.chii2.mqtt.common.codec;

import io.netty.buffer.ByteBuf;
import org.chii2.mqtt.common.message.UnsubscribeMessage;

/**
 * UNSUBSCRIBE Message Decode
 */
public class UnsubscribeDecoder extends BaseDecoder<UnsubscribeMessage> {

    @Override
    protected UnsubscribeMessage createMessage() {
        return new UnsubscribeMessage();
    }

    @Override
    protected void decodeVariableHeader(UnsubscribeMessage message, ByteBuf in) {
        // Variable Header
        // Message ID
        message.setMessageID(in.readUnsignedShort());
    }

    @Override
    protected void decodePayload(UnsubscribeMessage message, ByteBuf in) {
        // Payload
        // Topics
        while (in.readableBytes() > 0) {
            message.addTopicName(Utils.decodeString("Topic Name", in));
        }
    }
}
