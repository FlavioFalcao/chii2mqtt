package org.chii2.mqtt.server.storage;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.chii2.mqtt.common.message.PubRelMessage;
import org.chii2.mqtt.common.message.PublishMessage;
import org.chii2.mqtt.common.message.SubscribeMessage;
import org.chii2.mqtt.common.message.UnsubscribeMessage;
import org.fusesource.hawtbuf.codec.StringCodec;
import org.fusesource.hawtdb.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;

/**
 * HawtDB based MQTT Message Storage
 */
public class HawtDBStorage {

    // The Logger
    private final Logger logger = LoggerFactory.getLogger(HawtDBStorage.class);
    // Transaction
    private Transaction tx;
    // Retain Message DB   Key:PublisherID+MessageID Value:PUBLISH Message
    private SortedIndex<String, StoredPublishMessage> retainDB;
    // QoS1 & QoS2 Publish Message DB   Key:PublisherID+MessageID Value:PUBLISH Message
    private SortedIndex<String, StoredPublishMessage> publishDB;
    // QoS2 Publish Message DB   Key:PublisherID+MessageID Value:PUBREL Message
    private SortedIndex<String, StoredPublishMessage> pubrelDB;
    // Subscribe DB   Key:Topic Value:Set of subscribed ClientID
    private SortedIndex<String, HashSet<StoredSubscriber>> subscribeDB;
    // Publish Messages to be sent to Client   Key: ClientID Value:Set of Publish Messages
    private SortedIndex<String, HashSet<StoredPublishMessage>> clientDB;

    // Message tobe sent to subscribers   Key: SubscriberID Value:(HashMap Key:PublisherID+MessageID Value:StoredMessage)
    private Index<String, HashMap<String, StoredMessage>> outboundMessageDB;

    public HawtDBStorage(String path) {
        // Opening a transactional page file.
        TxPageFileFactory factory = new TxPageFileFactory();
        factory.setFile(new File(path));
        factory.open();
        TxPageFile pageFile = factory.getTxPageFile();
        tx = pageFile.tx();
        // Init database
        MultiIndexFactory multiIndexFactory = new MultiIndexFactory(tx);
        BTreeIndexFactory<String, StoredPublishMessage> retainFactory = new BTreeIndexFactory<>();
        retainFactory.setKeyCodec(StringCodec.INSTANCE);
        retainDB = (SortedIndex<String, StoredPublishMessage>) multiIndexFactory.openOrCreate("RETAIN", retainFactory);
        BTreeIndexFactory<String, StoredPublishMessage> publishFactory = new BTreeIndexFactory<>();
        publishFactory.setKeyCodec(StringCodec.INSTANCE);
        publishDB = (SortedIndex<String, StoredPublishMessage>) multiIndexFactory.openOrCreate("PUBLISH", publishFactory);
        BTreeIndexFactory<String, StoredPublishMessage> pubrelFactory = new BTreeIndexFactory<>();
        pubrelFactory.setKeyCodec(StringCodec.INSTANCE);
        pubrelDB = (SortedIndex<String, StoredPublishMessage>) multiIndexFactory.openOrCreate("PUBREL", pubrelFactory);
        BTreeIndexFactory<String, HashSet<StoredSubscriber>> subscribeFactory = new BTreeIndexFactory<>();
        subscribeFactory.setKeyCodec(StringCodec.INSTANCE);
        subscribeDB = (SortedIndex<String, HashSet<StoredSubscriber>>) multiIndexFactory.openOrCreate("SUBSCRIBE", subscribeFactory);
        BTreeIndexFactory<String, HashSet<StoredPublishMessage>> clientFactory = new BTreeIndexFactory<>();
        clientFactory.setKeyCodec(StringCodec.INSTANCE);
        clientDB = (SortedIndex<String, HashSet<StoredPublishMessage>>) multiIndexFactory.openOrCreate("CLIENT", clientFactory);

        HashIndexFactory<String, HashMap<String, StoredMessage>> outboundMessageFactory = new HashIndexFactory<>();
        outboundMessageFactory.setKeyCodec(StringCodec.INSTANCE);
        outboundMessageDB = multiIndexFactory.openOrCreate("OUTBOUND_MESSAGE", outboundMessageFactory);
    }

    /**
     * Put PUBLISH Message into Retain Database
     *
     * @param clientID Publisher ClientID
     * @param message  Publish Message
     */
    public void putRetain(String clientID, PublishMessage message) {
        retainDB.put(clientID + message.getMessageID(), new StoredPublishMessage(clientID, message));
        try {
            tx.commit();
            tx.flush();
        } catch (OptimisticUpdateException e) {
            logger.warn("Failed to put PUBLISH message {} from {} into retain storage: {}", message.getMessageID(), clientID, ExceptionUtils.getMessage(e));
        }
    }

    /**
     * Whether Retain Storage contains given PUBLISH Message
     *
     * @param clientID Publisher ClientID
     * @param message  Publish Message
     * @return True if storage contains this message
     */
    public boolean containRetain(String clientID, PublishMessage message) {
        String id = clientID + message.getMessageID();
        return retainDB.containsKey(id);
    }

    /**
     * Put PUBLISH Message into Publish Database used by QoS1 & QoS2 Messages
     *
     * @param clientID Publisher ClientID
     * @param message  Publish Message
     */
    public void putPublish(String clientID, PublishMessage message) {
        publishDB.put(clientID + message.getMessageID(), new StoredPublishMessage(clientID, message));
        try {
            tx.commit();
            tx.flush();
        } catch (OptimisticUpdateException e) {
            logger.warn("Failed to put PUBLISH message {} from {} into publish|client storage: {}", message.getMessageID(), clientID, ExceptionUtils.getMessage(e));
        }
    }

    /**
     * Whether Publish Storage contains given PUBLISH Message
     *
     * @param clientID Publisher ClientID
     * @param message  Publish Message
     * @return True if storage contains this message
     */
    public boolean containPublish(String clientID, PublishMessage message) {
        String id = clientID + message.getMessageID();
        return publishDB.containsKey(id);
    }

    /**
     * Put PUBREL Message into PubRel Database used by QoS2 Messages
     *
     * @param clientID Publisher ClientID
     * @param message  PubRel Message
     */
    public void putPubRel(String clientID, PubRelMessage message) {
        pubrelDB.put(clientID + message.getMessageID(), new StoredPublishMessage(clientID, message.getMessageID()));
        try {
            tx.commit();
            tx.flush();
        } catch (OptimisticUpdateException e) {
            logger.warn("Failed to put PUBREL message {} from {} into publrel storage: {}", message.getMessageID(), clientID, ExceptionUtils.getMessage(e));
        }
    }

    /**
     * Whether PubRel Storage contains given PUBLISH MessageID
     *
     * @param clientID  Publisher ClientID
     * @param messageID MessageID
     * @return True if storage contains this message
     */
    public boolean containPubRel(String clientID, int messageID) {
        String id = clientID + messageID;
        return pubrelDB.containsKey(id);
    }

    /**
     * Put SUBSCRIBE Message into Subscribe Database
     *
     * @param clientID Subscriber ClientID
     * @param message  Subscribe Message
     */
    public void putSubscribe(String clientID, SubscribeMessage message) {
        for (SubscribeMessage.Topic topic : message.getTopics()) {
            StoredSubscriber subscriber = new StoredSubscriber(clientID, topic.getQosLevel().getQoSValue());
            HashSet<StoredSubscriber> set;
            if (subscribeDB.containsKey(topic.getTopicName())) {
                set = subscribeDB.get(topic.getTopicName());
                if (set.contains(subscriber)) {
                    set.remove(subscriber);
                }
            } else {
                set = new HashSet<>();
            }
            set.add(subscriber);
            subscribeDB.put(topic.getTopicName(), set);
        }
        try {
            tx.commit();
            tx.flush();
        } catch (OptimisticUpdateException e) {
            logger.warn("Failed to put SUBSCRIBE message {} from {} into subscribe storage: {}", message.getMessageID(), clientID, ExceptionUtils.getMessage(e));
        }
    }

    /**
     * Whether Subscribe Storage contains given SUBSCRIBE Message
     *
     * @param clientID Subscriber ClientID
     * @param message  Subscribe Message
     * @return True if storage contains this message
     */
    public boolean containSubscribe(String clientID, SubscribeMessage message) {
        for (SubscribeMessage.Topic topic : message.getTopics()) {
            if (subscribeDB.containsKey(topic.getTopicName())) {
                StoredSubscriber subscriber = new StoredSubscriber(clientID, topic.getQosLevel().getQoSValue());
                HashSet<StoredSubscriber> set = subscribeDB.get(topic.getTopicName());
                if (!set.contains(subscriber)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Remove subscribes from Subscribe Storage with given ClientID & Topics
     *
     * @param clientID Unsubscriber ClientID
     * @param message  UNSUBSCRIBE Message
     */
    public void removeSubscribe(String clientID, UnsubscribeMessage message) {
        for (String topic : message.getTopicNames()) {
            if (subscribeDB.containsKey(topic)) {
                // FAKE QoS Level
                StoredSubscriber subscriber = new StoredSubscriber(clientID, 0);
                HashSet<StoredSubscriber> set = subscribeDB.get(topic);
                set.remove(subscriber);
                subscribeDB.put(topic, set);
            }
        }
        try {
            tx.commit();
            tx.flush();
        } catch (OptimisticUpdateException e) {
            logger.warn("Failed to apply UNSUBSCRIBE message {} from {} to subscribe storage: {}", message.getMessageID(), clientID, ExceptionUtils.getMessage(e));
        }
    }

    /**
     * Put PUBLISH Message into Client Database
     * Which means this message will be published to this client/subscriber
     *
     * @param clientID Publisher ClientID
     * @param message  Publish Message
     */
    public void putClient(String clientID, PublishMessage message) {
        StoredPublishMessage storedPublishMessage = new StoredPublishMessage(clientID, message);
        HashSet<StoredPublishMessage> set;
        if (clientDB.containsKey(clientID)) {
            set = clientDB.get(clientID);
            if (set.contains(storedPublishMessage)) {
                set.remove(storedPublishMessage);
            }
        } else {
            set = new HashSet<>();
        }
        set.add(storedPublishMessage);
        clientDB.put(clientID, set);
        try {
            tx.commit();
            tx.flush();
        } catch (OptimisticUpdateException e) {
            logger.warn("Failed to put PUBLISH message {} from {} into client storage: {}", message.getMessageID(), clientID, ExceptionUtils.getMessage(e));
        }
    }

    /**
     * Whether client Storage contains given PUBLISH Message
     *
     * @param clientID  Subscriber ClientID
     * @param messageID PUBLISH MessageID
     * @return True if storage contains this message
     */
    public boolean containClient(String clientID, int messageID) {
        StoredPublishMessage storedPublishMessage = new StoredPublishMessage(clientID, messageID);
        if (clientDB.containsKey(clientID)) {
            HashSet<StoredPublishMessage> set = clientDB.get(clientID);
            if (set.contains(storedPublishMessage)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Remove PUBLISH Message into Client Database
     * Which means this message already published to this client/subscriber
     *
     * @param clientID  Publisher ClientID
     * @param messageID Publish MessageID
     */
    public void removeClient(String clientID, int messageID) {
        StoredPublishMessage storedPublishMessage = new StoredPublishMessage(clientID, messageID);
        if (clientDB.containsKey(clientID)) {
            HashSet<StoredPublishMessage> set = clientDB.get(clientID);
            set.remove(storedPublishMessage);
            clientDB.put(clientID, set);
            try {
                tx.commit();
                tx.flush();
            } catch (OptimisticUpdateException e) {
                logger.warn("Failed to remove PUBLISH message {} from {} into client storage: {}", messageID, clientID, ExceptionUtils.getMessage(e));
            }
        }
    }

    public void putOutboundMessage(String subscriberID, String publisherID, boolean retain, int qosLevel, String topicName, int messageID, byte[] content) {
        StoredMessage message = new StoredMessage(publisherID, retain, qosLevel, topicName, messageID, content);
        HashMap<String, StoredMessage> map = outboundMessageDB.get(subscriberID);
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(publisherID + messageID, message);
        outboundMessageDB.put(subscriberID, map);
        try {
            tx.commit();
            tx.flush();
        } catch (OptimisticUpdateException e) {
            logger.warn("Outbound Message Storage failed to put  message {} send to {}: {}", messageID, subscriberID, ExceptionUtils.getMessage(e));
        }
    }

    public StoredMessage getOutboundMessage(String subscriberID, String publisherID, int messageID) {
        StoredMessage result = null;
        HashMap<String, StoredMessage> map = outboundMessageDB.get(subscriberID);
        if (map != null) {
            result = map.get(publisherID + messageID);
        }
        return result;
    }

    public void removeOutboundMessage(String subscriberID, String publisherID, int messageID) {
        HashMap<String, StoredMessage> map = outboundMessageDB.get(subscriberID);
        if (map != null) {
            map.remove(publisherID + messageID);
            if (map.isEmpty()) {
                outboundMessageDB.remove(subscriberID);
            } else {
                outboundMessageDB.put(subscriberID, map);
            }
            try {
                tx.commit();
                tx.flush();
            } catch (OptimisticUpdateException e) {
                logger.warn("Outbound Message Storage failed to remove message {} send to {}: {}", messageID, subscriberID, ExceptionUtils.getMessage(e));
            }
        }
    }

    public boolean containsOutboundMessage(String subscriberID, String publisherID, int messageID) {
        boolean result = false;
        HashMap<String, StoredMessage> map = outboundMessageDB.get(subscriberID);
        if (map != null && map.containsKey(publisherID + messageID)) {
            result = true;
        }
        return result;
    }
}
