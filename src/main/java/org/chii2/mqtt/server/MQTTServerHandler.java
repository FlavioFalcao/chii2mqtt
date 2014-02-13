package org.chii2.mqtt.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import org.chii2.mqtt.common.message.MQTTMessage;
import org.chii2.mqtt.server.disruptor.InboundDisruptor;
import org.chii2.mqtt.server.disruptor.InboundMQTTEventTranslator;

/**
 * Netty Server Handler
 */
public class MQTTServerHandler extends ChannelInboundHandlerAdapter {

    // Inbound Disruptor
    InboundDisruptor disruptor;
    // State Variable Key
    public static final AttributeKey<String> CLIENT_ID = AttributeKey.valueOf("CLIENT_ID");

    public MQTTServerHandler(InboundDisruptor disruptor) {
        this.disruptor = disruptor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        // Received the MQTT Message , push to disruptor
        disruptor.pushEvent(new InboundMQTTEventTranslator(ctx, (MQTTMessage) message));
    }
}
