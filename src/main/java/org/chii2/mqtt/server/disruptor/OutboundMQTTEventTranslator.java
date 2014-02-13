package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventTranslator;
import org.chii2.mqtt.common.message.MQTTMessage;

/**
 * MQTT Event Translator used by OutboundDisruptor
 */
public class OutboundMQTTEventTranslator implements EventTranslator<OutboundMQTTEvent> {

    // Publisher's Client ID
    private String publisherID;
    // Subscriber's Client ID
    private String subscriberID;
    // PUBLISH Message
    private MQTTMessage message;

    public OutboundMQTTEventTranslator(String publisherID, String subscriberID, MQTTMessage message) {
        this.publisherID = publisherID;
        this.subscriberID = subscriberID;
        this.message = message;
    }

    @Override
    public void translateTo(OutboundMQTTEvent event, long sequence) {
        event.setPublisherID(publisherID);
        event.setSubscriberID(subscriberID);
        event.setMessage(message);
    }
}
