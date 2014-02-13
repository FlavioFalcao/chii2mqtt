package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventFactory;
import org.chii2.mqtt.common.message.MQTTMessage;

/**
 * Event used by InboundDisruptor
 */
public class OutboundMQTTEvent {

    // Publisher's Client ID
    private String publisherID;
    // Subscriber's Client ID
    private String subscriberID;
    // Has sent at least once
    private boolean resend;
    // Last sending time in milliseconds
    private long sendingTime;
    // PUBLISH Message
    private MQTTMessage message;

    /**
     * Event Factory used by disruptor
     */
    public final static EventFactory<OutboundMQTTEvent> factory = new EventFactory<OutboundMQTTEvent>() {
        public OutboundMQTTEvent newInstance() { return new OutboundMQTTEvent(); }
    };

    public String getPublisherID() {
        return publisherID;
    }

    public void setPublisherID(String publisherID) {
        this.publisherID = publisherID;
    }

    public String getSubscriberID() {
        return subscriberID;
    }

    public void setSubscriberID(String subscriberID) {
        this.subscriberID = subscriberID;
    }

    public boolean isResend() {
        return resend;
    }

    public void setResend(boolean resend) {
        this.resend = resend;
    }

    public long getSendingTime() {
        return sendingTime;
    }

    public void setSendingTime(long sendingTime) {
        this.sendingTime = sendingTime;
    }

    public MQTTMessage getMessage() {
        return message;
    }

    public void setMessage(MQTTMessage message) {
        this.message = message;
    }
}
