package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.chii2.mqtt.common.message.MQTTMessage;
import org.chii2.mqtt.common.message.PublishMessage;
import org.chii2.mqtt.server.storage.ChannelRepository;
import org.chii2.mqtt.server.storage.HawtDBStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outbound Processor used by OutboundDisruptor
 * Send outbound MQTT Publish Messages to Subscribers
 */
public class OutboundProcessor implements EventHandler<OutboundMQTTEvent> {

    // Storage
    private HawtDBStorage storage;
    // The Logger
    private final Logger logger = LoggerFactory.getLogger(OutboundProcessor.class);

    public OutboundProcessor(HawtDBStorage storage) {
        this.storage = storage;
    }

    @Override
    public void onEvent(OutboundMQTTEvent event, long sequence, boolean endOfBatch) throws Exception {
        MQTTMessage message = event.getMessage();
        if (message instanceof PublishMessage) {
            onPublish(event, sequence, endOfBatch);
        }
    }

    /**
     * Received a Publish event, try to publish the message to subscriber
     *
     * @param event Outbound MQTT Event
     */
    public void onPublish(OutboundMQTTEvent event, long sequence, boolean endOfBatch) {
        PublishMessage message = (PublishMessage) event.getMessage();
        String subscriberID = event.getSubscriberID();
        String publisherID = event.getPublisherID();
        int messageID = message.getMessageID();
        // Logic 1:     The subscriber is connected to the server
        // Logic 1.1:   The storage contains the message, send the message and record the sending time
        // Logic 1.1.1: Message QoS is 0, remove from storage and mark the event shouldn't resend
        // Logic 1.1.2: Message QoS is 1|2, mark the event should resend
        // Logic 1.2:   The storage doesn't contain the message, it must been acknowledged, mark the event shouldn't resend
        // Logic 2:     The subscriber is NOT connected to the server, mark the event shouldn't resend
        if (ChannelRepository.containsClientChannel(subscriberID)) {
            ChannelHandlerContext context = ChannelRepository.getClientChannel(subscriberID);
            if (context.channel().isActive()) {
                if (storage.containsOutboundMessage(subscriberID, publisherID, messageID)) {
                    sendMessage(context, message, subscriberID);
                    event.setSendingTime(System.currentTimeMillis());
                    if (message.getQosLevel() == MQTTMessage.QoSLevel.MOST_ONCE) {
                        storage.removeOutboundMessage(subscriberID, publisherID, messageID);
                        event.setResend(false);
                    } else {
                        event.setResend(true);
                    }
                } else {
                    event.setResend(false);
                }
            } else {
                ChannelRepository.removeClientChannel(subscriberID);
                event.setResend(false);
            }
        } else {
            event.setResend(false);
        }
    }

    /**
     * Send MQTT Publish Message to subscribers
     * Because using Netty, this should not blocking
     *
     * @param context      ChannelHandlerContext
     * @param message      MQTT Publish Message
     * @param subscriberID Subscriber ID
     */
    protected void sendMessage(ChannelHandlerContext context, final PublishMessage message, final String subscriberID) {
        ChannelFuture future = context.writeAndFlush(message);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("{} Message {} has been sent to {}.", message.getMessageType(), message.getMessageID(), subscriberID);
            }
        });
    }
}
