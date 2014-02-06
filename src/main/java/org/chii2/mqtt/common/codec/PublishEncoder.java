package org.chii2.mqtt.common.codec;

import io.netty.buffer.ByteBuf;
import org.chii2.mqtt.common.message.MQTTMessage;
import org.chii2.mqtt.common.message.PublishMessage;

/**
 * PUBLISH Message Encoder
 */
public class PublishEncoder extends BaseEncoder<PublishMessage> {

    @Override
    protected void encodeVariableHeader(PublishMessage message, ByteBuf out) {
        // Write Variable Header
        out.writeBytes(Utils.encodeString("Topic Name", message.getTopicName()));
        if (message.getQosLevel() == MQTTMessage.QoSLevel.LEAST_ONCE ||
                message.getQosLevel() == MQTTMessage.QoSLevel.EXACTLY_ONCE) {
            out.writeShort(message.getMessageID());
        }
    }

    @Override
    protected void encodePayload(PublishMessage message, ByteBuf out) {
        // Write Payload
        out.writeBytes(message.getContent());
    }
}
