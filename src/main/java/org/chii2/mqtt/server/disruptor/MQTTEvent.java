package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventFactory;
import io.netty.channel.ChannelHandlerContext;
import org.chii2.mqtt.common.message.MQTTMessage;

/**
 * Event used by Disruptor
 */
public class MQTTEvent {

    private ChannelHandlerContext context;
    private MQTTMessage mqttMessage;

    public ChannelHandlerContext getContext() {
        return context;
    }

    public void setContext(ChannelHandlerContext context) {
        this.context = context;
    }

    public MQTTMessage getMQTTMessage() {
        return mqttMessage;
    }

    public void setMQTTMessage(MQTTMessage mqttMessage) {
        this.mqttMessage = mqttMessage;
    }

    /**
     * Event Factory used by disruptor
     */
    public final static EventFactory<MQTTEvent> factory = new EventFactory<MQTTEvent>() {
        public MQTTEvent newInstance() { return new MQTTEvent(); }
    };
}
