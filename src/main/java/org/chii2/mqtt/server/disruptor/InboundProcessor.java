package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import org.chii2.mqtt.common.message.*;
import org.chii2.mqtt.server.MQTTServerHandler;
import org.chii2.mqtt.server.storage.HawtDBStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * MQTT Message Inbound Journal Processor
 */
@SuppressWarnings("unused")
public class InboundProcessor implements EventHandler<InboundMQTTEvent> {
    // The Logger
    private final Logger logger = LoggerFactory.getLogger(InboundProcessor.class);
    // Storage
    private HawtDBStorage storage;

    public InboundProcessor(HawtDBStorage storage) {
        this.storage = storage;
    }

    @Override
    public void onEvent(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) throws Exception {
        MQTTMessage message = event.getMQTTMessage();
        if (message instanceof ConnectMessage) {
            onConnect(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PublishMessage) {
            onPublish(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PubAckMessage) {
            onPubAck(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PubRelMessage) {
            onPubRel(event, sequenceNumber, endOfBatch);
        } else if (message instanceof PubCompMessage) {
            onPubComp(event, sequenceNumber, endOfBatch);
        } else if (message instanceof SubscribeMessage) {
            onSubscribe(event, sequenceNumber, endOfBatch);
        } else if (message instanceof UnsubscribeMessage) {
            onUnsubscribe(event, sequenceNumber, endOfBatch);
        }
    }

    /**
     * Received MQTT CONNECT Message from a client
     *
     * @param event          InboundMQTTEvent which contains a CONNECT Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onConnect(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ConnectMessage connectMessage = (ConnectMessage) event.getMQTTMessage();
        ConnAckMessage connAckMessage;
        // Unacceptable Protocol Version
        if (!connectMessage.isAcceptableProtocolVersion()) {
            connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.UNACCEPTABLE_PROTOCOL_VERSION);
        }
        // Unacceptable Client ID
        else if (connectMessage.getClientID() == null || connectMessage.getClientID().getBytes().length > 23) {
            connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.IDENTIFIER_REJECTED);
        }
        // User Name and Password Provided
        else if (connectMessage.isUserNameFlag()) {
            // Authorized
            if (isAuthorized(connectMessage.getClientID(), connectMessage.getUserName(), connectMessage.getPassword())) {
                if (connectMessage.isCleanSession()) {
                    // TODO: Handle Clean Session
                }

                if (connectMessage.isWillFlag()) {
                    // TODO: Handle Will
                }

                // TODO: Keep Alive Timer

                connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.CONNECTION_ACCEPTED);
            }
            // Not Authorized
            else {
                connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.BAD_USERNAME_OR_PASSWORD);
            }
        } else {
            connAckMessage = new ConnAckMessage(ConnAckMessage.ReturnCode.NOT_AUTHORIZED);
        }
    }

    private boolean isAuthorized(String clientID, String userName, String password) {
        return true;
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
        Attribute<String> attr = context.attr(MQTTServerHandler.CLIENT_ID);
        String clientID = attr.get();
        PublishMessage publishMessage = (PublishMessage) event.getMQTTMessage();
        MQTTMessage.QoSLevel qos = publishMessage.getQosLevel();
        // Retain flag is only used on PUBLISH messages. When a client sends a PUBLISH to a
        // server, if the Retain flag is set (1), the server should hold on to the message after
        // it has been delivered to the current subscribers.
        // When a new subscription is established on a topic, the last retained message on
        // that topic should be sent to the subscriber with the Retain flag set. If there is no
        // retained message, nothing is sent
        // When a server sends a PUBLISH to a client as a result of a subscription that
        // already existed when the original PUBLISH arrived, the Retain flag should not be
        // set, regardless of the Retain flag of the original PUBLISH. This allows a client to
        // distinguish messages that are being received because they were retained and
        // those that are being received "live".
        // Retained messages should be kept over restarts of the server.
        // A server may delete a retained message if it receives a message with a zero-length
        // payload and the Retain flag set on the same topic.
        if (publishMessage.isRetain()) {
            // Save the message to persistent storage as retain message
            storage.putRetain(clientID, publishMessage);
        }

        if (publishMessage.isDupFlag()) {
            // Mark the event as duplicated if this message already in storage
            if (storage.containPublish(clientID, publishMessage)) {
                event.setDuplicated(true);
            }
        }
        if (!event.isDuplicated() && (qos == MQTTMessage.QoSLevel.LEAST_ONCE || qos == MQTTMessage.QoSLevel.EXACTLY_ONCE)) {
            // Save the message to persistent storage
            storage.putPublish(clientID, publishMessage);
        }
    }

    /**
     * Received MQTT PUBACK Message from a subscriber in response to a PUBLISH message from the server
     * Step1: QoS 1 PUBLISH Message send from Sever to Subscriber
     * Step2: PUBACK Message send from Subscriber to Server
     *
     * @param event          InboundMQTTEvent which contains a PUBACK Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onPubAck(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        Attribute<String> attr = context.attr(MQTTServerHandler.CLIENT_ID);
        String clientID = attr.get();
        PubAckMessage pubAckMessage = (PubAckMessage) event.getMQTTMessage();
        // Mark the ClientID has received MessageID in persistent storage
        storage.removeClient(clientID, pubAckMessage.getMessageID());
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
        Attribute<String> attr = context.attr(MQTTServerHandler.CLIENT_ID);
        String clientID = attr.get();
        PubRelMessage pubRelMessage = (PubRelMessage) event.getMQTTMessage();
        // Publisher send duplicated PUBREL Message, because it didn't received PUBCOMP Message from Server
        if (pubRelMessage.isDupFlag()) {
            // Mark the event as duplicated if this message already in storage
            if (storage.containPubRel(clientID, pubRelMessage.getMessageID())) {
                event.setDuplicated(true);
            }
        }
        if (!event.isDuplicated()) {
            // Save the message to persistent storage
            storage.putPubRel(clientID, pubRelMessage);
        }
    }

    /**
     * Received MQTT PUBCOMP Message from a subscriber to a PUBREL message from the server
     * Step1: QoS 1 PUBLISH Message send from Sever to Subscriber
     * Step2: PUBREC Message send from Subscriber to Server
     * Step3: PUBREL Message send from Server to Subscriber
     * Step4: PUBCOMP Message send from Subscriber to Server
     *
     * @param event          InboundMQTTEvent which contains a PUBCOMP Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onPubComp(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch) {
        ChannelHandlerContext context = event.getContext();
        Attribute<String> attr = context.attr(MQTTServerHandler.CLIENT_ID);
        String clientID = attr.get();
        PubCompMessage pubCompMessage = (PubCompMessage) event.getMQTTMessage();
        // Mark the ClientID has received MessageID in persistent storage
        storage.removeClient(clientID, pubCompMessage.getMessageID());
    }

    /**
     * Received MQTT SUBSCRIBE Message from a subscriber
     * The SUBSCRIBE message allows a client to register an interest in one or more topic
     * names with the server. Messages published to these topics are delivered from the
     * server to the client as PUBLISH messages. The SUBSCRIBE message also specifies the
     * QoS level at which the subscriber wants to receive published messages.
     *
     * Assuming that the requested QoS level is granted, the client receives PUBLISH
     * messages at less than or equal to this level, depending on the QoS level of the original
     * message from the publisher. For example, if a client has a QoS level 1 subscription to a
     * particular topic, then a QoS level 0 PUBLISH message to that topic is delivered to the
     * client at QoS level 0. A QoS level 2 PUBLISH message to the same topic is downgraded
     * to QoS level 1 for delivery to the client.

     * @param event          InboundMQTTEvent which contains a SUBSCRIBE Message
     * @param sequenceNumber Disruptor sequence number
     * @param endOfBatch     Disruptor is end of batch
     */
    protected void onSubscribe(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch){
        ChannelHandlerContext context = event.getContext();
        Attribute<String> attr = context.attr(MQTTServerHandler.CLIENT_ID);
        String clientID = attr.get();
        SubscribeMessage subscribeMessage = (SubscribeMessage) event.getMQTTMessage();
        List<SubscribeMessage.Topic> topics = subscribeMessage.getTopics();
        if (subscribeMessage.isDupFlag()) {
            // If all topics were subscribed by this client, mark the event duplicated
            if (storage.containSubscribe(clientID, subscribeMessage)) {
                event.setDuplicated(true);
            }
        }
        // Change the QoS Level based on Server side logic
        for (SubscribeMessage.Topic topic : topics) {
            topic.setQosLevel(getGrantedQoS(context, topic.getTopicName(), topic.getQosLevel()));
        }
        if (!event.isDuplicated()) {
            // Save the ClientID to subscription persistent storage with given Topics
            storage.putSubscribe(clientID, subscribeMessage);
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
    protected void onUnsubscribe(InboundMQTTEvent event, long sequenceNumber, boolean endOfBatch){
        ChannelHandlerContext context = event.getContext();
        Attribute<String> attr = context.attr(MQTTServerHandler.CLIENT_ID);
        String clientID = attr.get();
        UnsubscribeMessage unsubscribeMessage = (UnsubscribeMessage) event.getMQTTMessage();
        storage.removeSubscribe(clientID, unsubscribeMessage);
    }

    /**
     * Determine Topic's granted QoS Level for given Client ID
     * Current logic simply returns the input QoS Level.
     * Override this method to provide vendor specific validation logic.
     *
     * @param context ChannelHandlerContext
     * @param topicName Topic Name client subscribed to
     * @param qos QoS Level client required
     * @return Granted QoS Level
     */
    protected MQTTMessage.QoSLevel getGrantedQoS(ChannelHandlerContext context, String topicName, MQTTMessage.QoSLevel qos) {
        return qos;
    }
}
