package org.chii2.mqtt.common.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.chii2.mqtt.common.message.MQTTMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MQTT Message Decoder
 * This will be called by Netty pipeline, and route to specific message decoder.
 */
public class MQTTDecoder extends ByteToMessageDecoder {

    // Message Type <---> Decoder Map
    protected Map<MQTTMessage.MessageType, BaseDecoder> decoderMap = new HashMap<>();

    public MQTTDecoder() {
        super();

        // Init Map
        decoderMap.put(MQTTMessage.MessageType.CONNECT, new ConnectDecoder());
        decoderMap.put(MQTTMessage.MessageType.CONNACK, new ConnAckDecoder());
        decoderMap.put(MQTTMessage.MessageType.PUBLISH, new PublishDecoder());
        decoderMap.put(MQTTMessage.MessageType.PUBACK, new PubAckDecoder());
        decoderMap.put(MQTTMessage.MessageType.SUBSCRIBE, new SubscribeDecoder());
        decoderMap.put(MQTTMessage.MessageType.SUBACK, new SubAckDecoder());
        decoderMap.put(MQTTMessage.MessageType.UNSUBSCRIBE, new UnsubscribeDecoder());
        decoderMap.put(MQTTMessage.MessageType.DISCONNECT, new DisconnectDecoder());
        decoderMap.put(MQTTMessage.MessageType.PINGREQ, new PingReqDecoder());
        decoderMap.put(MQTTMessage.MessageType.PINGRESP, new PingRespDecoder());
        decoderMap.put(MQTTMessage.MessageType.UNSUBACK, new UnsubAckDecoder());
        decoderMap.put(MQTTMessage.MessageType.PUBCOMP, new PubCompDecoder());
        decoderMap.put(MQTTMessage.MessageType.PUBREC, new PubRecDecoder());
        decoderMap.put(MQTTMessage.MessageType.PUBREL, new PubRelDecoder());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();

        // Calculate Message Type
        byte firstByte = in.readByte();
        MQTTMessage.MessageType messageType = MQTTMessage.MessageType.values()[(firstByte & 0x00F0) >> 4];

        in.resetReaderIndex();

        // Decode
        BaseDecoder decoder = decoderMap.get(messageType);
        decoder.decode(ctx, in, out);
    }
}
