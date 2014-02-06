package org.chii2.mqtt.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.chii2.mqtt.common.message.MQTTMessage;
import org.chii2.mqtt.server.disruptor.InboundDisruptor;
import org.chii2.mqtt.server.disruptor.MQTTEventTranslator;

/**
 * Netty Server Handler
 */
public class MQTTServerHandler extends ChannelInboundHandlerAdapter {

    InboundDisruptor disruptor;

    public MQTTServerHandler(InboundDisruptor disruptor) {
        this.disruptor = disruptor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        disruptor.pushEvent(new MQTTEventTranslator(ctx, (MQTTMessage) message));
    }
}
