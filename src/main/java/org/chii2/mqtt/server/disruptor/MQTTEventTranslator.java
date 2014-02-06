package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventTranslator;
import io.netty.channel.ChannelHandlerContext;
import org.chii2.mqtt.common.message.MQTTMessage;

/**
 * MQTT Event Translator used by Disruptor
 */
public class MQTTEventTranslator implements EventTranslator<MQTTEvent> {

    private ChannelHandlerContext context;
    private MQTTMessage message;

    public MQTTEventTranslator(ChannelHandlerContext context, MQTTMessage message) {
        this.context = context;
        this.message = message;
    }

    @Override
    public void translateTo(MQTTEvent event, long sequence) {
        event.setContext(context);
        event.setMQTTMessage(message);
    }
}
