package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventHandler;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.chii2.mqtt.common.message.MQTTMessage;
import org.chii2.mqtt.common.message.PublishMessage;
import org.chii2.mqtt.server.storage.HawtDBStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outbound Processor used by OutboundDisruptor
 * Try resend the message if client not acknowledged
 */
public class OutboundResendProcessor implements EventHandler<OutboundMQTTEvent> {

    // Interval time to resend the message
    private long interval;
    // Storage
    private HawtDBStorage storage;
    // Outbound Disruptor
    private OutboundDisruptor disruptor;
    // The Logger
    private final Logger logger = LoggerFactory.getLogger(OutboundResendProcessor.class);

    public OutboundResendProcessor(long interval, HawtDBStorage storage, OutboundDisruptor disruptor) {
        this.interval = interval;
        this.storage = storage;
        this.disruptor = disruptor;
    }

    @Override
    public void onEvent(OutboundMQTTEvent event, long sequence, boolean endOfBatch) throws Exception {
        MQTTMessage message = event.getMessage();
        if (message instanceof PublishMessage) {
            onPublish(event, sequence, endOfBatch);
        }
    }

    /**
     * Received a Publish event, try to resend the message after specific time
     *
     * @param event Outbound MQTT Event
     */
    public void onPublish(OutboundMQTTEvent event, long sequence, boolean endOfBatch) {
        PublishMessage message = (PublishMessage) event.getMessage();
        String subscriberID = event.getSubscriberID();
        String publisherID = event.getPublisherID();
        int messageID = message.getMessageID();
        // Logic 1:   If event is marked as resend, sleep the specific interval time
        // Logic 1.1: If storage contains the message, re-add the event to the disruptor
        // Logic 1.2: If storage doesn't contains the message, it must been acknowledged, discard the event
        // Logic 2:   If event isn't marked as resend, discard it
        if (event.isResend()) {
            long sleepTime = interval - (System.currentTimeMillis() - event.getSendingTime());
            if (sleepTime > 0) {
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    logger.error("Outbound Resend Processor interrupted {}:", ExceptionUtils.getMessage(e));
                }
            }
            if (storage.containsOutboundMessage(subscriberID, publisherID, messageID)) {
                disruptor.pushEvent(new OutboundMQTTEventTranslator(publisherID, subscriberID, message));
                logger.info("Re-Add {} Message {} should be sent to {} to outbound message queue.", message.getMessageType(), message.getMessageID(), subscriberID);
            }
        }
    }
}
