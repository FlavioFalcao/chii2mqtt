package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.chii2.mqtt.common.message.*;
import org.chii2.mqtt.server.storage.HawtDBStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * MQTT Message Core Logic Processor
 */
public class LogicProcessor implements EventHandler<InboundMQTTEvent> {
    // The Logger
    private final Logger logger = LoggerFactory.getLogger(LogicProcessor.class);

    @Override
    public void onEvent(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) throws Exception {
        MQTTMessage message = event.getMQTTMessage();
        if (message instanceof ConnectMessage) {
            onConnect(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PublishMessage) {
            onPublish(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PubRecMessage) {
            onPubRec(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PubRelMessage) {
            onPubRel(event, sequenceNumber, endOfBatch);
        } else if (message instanceof SubscribeMessage) {
            onSubscribe(event, sequenceNumber, endOfBatch);
        } else if (message instanceof UnsubscribeMessage) {
            onUnsubscribe(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PingReqMessage) {
            onPingReq(event, sequenceNumber, endOfBatch);
        } else if (message instanceof DisconnectMessage) {
            onDisconnect(event, sequenceNumber, endOfBatch);
        }
    }

    protected void onConnect(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        ConnectMessage connectMessage = (ConnectMessage) event.getMQTTMessage();
        ConnAckMessage connAckMessage;
        // Unacceptable Protocol Version
        if (!connectMessage.isAcceptableProtocolVersion()) {
            connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.UNACCEPTABLE_PROTOCOL_VERSION);
        }
        // Unacceptable Client ID
        if (connectMessage.getClientID() == null || connectMessage.getClientID().getBytes().length > 23) {
            connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.IDENTIFIER_REJECTED);
        }

        // TODO: Authorization

        if (connectMessage.isCleanSession()) {
            // TODO: Handle Clean Session
        }

        if (connectMessage.isWillFlag()) {
            // TODO: Handle Will
        }

        // TODO: Keep Alive Timer

        connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.CONNECTION_ACCEPTED);
        // Send message
        sendMessage(context, connAckMessage);
    }

    /**
     * Received MQTT PUBLISH Message from a publisher
     * The action of the recipient when it receives a message depends on the
     * QoS level of the message:
     * QoS 0
     * Make the message available to any interested parties.
     * QoS 1
     * Log the message to persistent storage, make it available to any interested parties,
     * and return a PUBACK message to the sender.
     * QoS 2
     * Log the message to persistent storage, do not make it available to interested
     * parties yet, and return a PUBREC message to the sender.
     * <p/>
     * If the server receives the message, interested parties means subscribers to the topic of
     * the PUBLISH message.
     *
     * @param event          InboundMQTTEvent which contains a PUBLISH Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onPublish(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        PublishMessage publishMessage = (PublishMessage) event.getMQTTMessage();
        MQTTMessage.QoSLevel qos = publishMessage.getQosLevel();
        if (qos == MQTTMessage.QoSLevel.MOST_ONCE) {
            // TODO: Notify OutboundDisruptor to publish message to clients.
        } else if (qos == MQTTMessage.QoSLevel.LEAST_ONCE) {
            PubAckMessage pubAckMessage = new PubAckMessage(publishMessage.getMessageID());
            sendMessage(context, pubAckMessage);
            if (!event.isDuplicated()) {
                // TODO: Notify OutboundDisruptor to publish message to clients.
            }
        } else if (qos == MQTTMessage.QoSLevel.EXACTLY_ONCE) {
            PubRecMessage pubRecMessage = new PubRecMessage(publishMessage.getMessageID());
            sendMessage(context, pubRecMessage);
        }
    }

    /**
     * Received MQTT PUBREC Message from a subscriber in response to a PUBLISH message from the server
     * Step1: QoS 2 PUBLISH Message send from Sever to Subscriber
     * Step2: PUBREC Message send from Subscriber to Server
     * <p/>
     * When it receives a PUBREC message, the recipient sends a PUBREL message to the
     * sender with the same Message ID as the PUBREC message.
     *
     * @param event          InboundMQTTEvent which contains a PUBREC Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    private void onPubRec(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        PubRecMessage pubRecMessage = (PubRecMessage) event.getMQTTMessage();
        // DUP flag is false, because it is the first time
        PubRelMessage pubRelMessage = new PubRelMessage(false, pubRecMessage.getMessageID());
        sendMessage(context, pubRelMessage);
    }

    /**
     * Received MQTT PUBREL Message from a publisher to a PUBREC message from the server
     * Step1: QoS 2 PUBLISH Message send from Publisher to Server
     * Step2: PUBREC Message send from Server to Publisher
     * Step3: PUBREL Message send from Publisher to Server
     * <p/>
     * When the server receives a PUBREL message from a publisher, the server makes the
     * original message available to interested subscribers, and sends a PUBCOMP message
     * with the same Message ID to the publisher.
     *
     * @param event          InboundMQTTEvent which contains a PUBREL Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onPubRel(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        PubRelMessage pubRelMessage = (PubRelMessage) event.getMQTTMessage();
        PubCompMessage pubCompMessage = new PubCompMessage(pubRelMessage.getMessageID());
        sendMessage(context, pubCompMessage);
        // If PUBREL Message is duplicate and Server already publish the message to the subscribers,
        // DO NOT PUBLISH AGAIN
        if (!event.isDuplicated()) {
            // TODO: Notify OutboundDisruptor to publish message to clients.
        }
    }

    /**
     * Received MQTT SUBSCRIBE Message from a subscriber
     * The SUBSCRIBE message allows a client to register an interest in one or more topic
     * names with the server. Messages published to these topics are delivered from the
     * server to the client as PUBLISH messages. The SUBSCRIBE message also specifies the
     * QoS level at which the subscriber wants to receive published messages.
     * <p/>
     * Assuming that the requested QoS level is granted, the client receives PUBLISH
     * messages at less than or equal to this level, depending on the QoS level of the original
     * message from the publisher. For example, if a client has a QoS level 1 subscription to a
     * particular topic, then a QoS level 0 PUBLISH message to that topic is delivered to the
     * client at QoS level 0. A QoS level 2 PUBLISH message to the same topic is downgraded
     * to QoS level 1 for delivery to the client.
     *
     * @param event          InboundMQTTEvent which contains a SUBSCRIBE Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onSubscribe(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        SubscribeMessage subscribeMessage = (SubscribeMessage) event.getMQTTMessage();
        List<SubscribeMessage.Topic> topics = subscribeMessage.getTopics();
        List<MQTTMessage.QoSLevel> grantedQoS = new ArrayList<>();
        for (SubscribeMessage.Topic topic : topics) {
            grantedQoS.add(topic.getQosLevel());
        }
        SubAckMessage subAckMessage = new SubAckMessage(subscribeMessage.getMessageID(), grantedQoS);
        sendMessage(context, subAckMessage);
        if (!event.isDuplicated()) {
            // TODO: Notify OutboundDisruptor to publish retain message with given Topics.
        }
    }

    /**
     * Received MQTT UNSUBSCRIBE Message from a subscriber
     * An UNSUBSCRIBE message is sent by the client to the server to unsubscribe from
     * named topics.
     *
     * @param event          InboundMQTTEvent which contains a UNSUBSCRIBE Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onUnsubscribe(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        UnsubscribeMessage unsubscribeMessage = (UnsubscribeMessage) event.getMQTTMessage();
        UnsubAckMessage unsubAckMessage = new UnsubAckMessage(unsubscribeMessage.getMessageID());
        sendMessage(context, unsubAckMessage);
    }

    /**
     * Received MQTT PINGREQ Message from a client
     * The PINGREQ message is an "are you alive?" message that is sent from a connected
     * client to the server.
     *
     * @param event          InboundMQTTEvent which contains a PINGREQ Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onPingReq(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        sendMessage(context, new PingRespMessage());
    }

    /**
     * Received MQTT DISCONNECT Message from a client
     * The DISCONNECT message is sent from the client to the server to indicate that it is
     * about to close its TCP/IP connection. This allows for a clean disconnection, rather than
     * just dropping the line.
     * If the client had connected with the clean session flag set, then all previously
     * maintained information about the client will be discarded.
     * A server should not rely on the client to close the TCP/IP connection after receiving a
     * DISCONNECT.
     *
     * @param event          InboundMQTTEvent which contains a DISCONNECT Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onDisconnect(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {

    }

    /**
     * Send MQTT Message back to client
     * Because using Netty, this should not blocking
     *
     * @param context ChannelHandlerContext
     * @param message MQTT Message
     */
    protected void sendMessage(ChannelHandlerContext context, final MQTTMessage message) {
        ChannelFuture future = context.writeAndFlush(message);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("{} Message has been sent.", message.getMessageType());
            }
        });
    }
}
