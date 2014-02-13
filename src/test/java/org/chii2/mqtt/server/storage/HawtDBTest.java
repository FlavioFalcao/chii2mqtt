package org.chii2.mqtt.server.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.chii2.mqtt.common.message.*;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * HawtDBStorage Test
 */
public class HawtDBTest {

    private HawtDBStorage storage;

    @BeforeClass
    public void setUp() {
        String storagePath = System.getProperty("java.io.tmpdir") + File.separator + ".chii2" + File.separator + "chii2mqtt.db";
        File file = new File(storagePath);
        if (file.exists()) {
            file.delete();
        }
        storage = new HawtDBStorage(storagePath);
    }

    @Test
    public void outboundMessageStorageTest() {
        // Simple
        storage.putOutboundMessage("SUBSCRIBER", "Chii2Server", false, 1, "\\MyTopic", 100, null);
        assert storage.containsOutboundMessage("SUBSCRIBER", "Chii2Server", 100);
        StoredMessage message = storage.getOutboundMessage("SUBSCRIBER", "Chii2Server", 100);
        assert message.getPublisherID().equals("Chii2Server");
        assert !message.isRetain();
        assert message.getQoSLevel() == 1;
        assert message.getTopicName().equals("\\MyTopic");
        assert message.getMessageID() == 100;
        // Duplicated insert
        storage.putOutboundMessage("SUBSCRIBER", "Chii2Server", false, 2, "\\MyTopic\\SubTopic", 100, null);
        assert storage.containsOutboundMessage("SUBSCRIBER", "Chii2Server", 100);
        message = storage.getOutboundMessage("SUBSCRIBER", "Chii2Server", 100);
        assert message.getQoSLevel() == 2;
        assert message.getTopicName().equals("\\MyTopic\\SubTopic");
        // Remove
        storage.removeOutboundMessage("SUBSCRIBER", "Chii2Server", 100);
        assert !storage.containsOutboundMessage("SUBSCRIBER", "Chii2Server", 100);
    }

    @Test
    public void retainStorageTest() throws UnsupportedEncodingException {
        ByteBuf buf = Unpooled.buffer();
        ByteBuffer content = ByteBuffer.allocate(100);
        content.put("A very long content. Blah blah blah blah blah!".getBytes("UTF-8"));
        PublishMessage message = new PublishMessage(true, MQTTMessage.QoSLevel.EXACTLY_ONCE, false, "\\Chii2\\Client", 77, content);
        storage.putRetain("TEST_CLIENT", message);
        assert storage.containRetain("TEST_CLIENT", message);
    }

    @Test
    public void publishStorageTest() throws UnsupportedEncodingException {
        ByteBuf buf = Unpooled.buffer();
        ByteBuffer content = ByteBuffer.allocate(100);
        content.put("A very long content. Blah blah blah blah blah!".getBytes("UTF-8"));
        PublishMessage message = new PublishMessage(true, MQTTMessage.QoSLevel.EXACTLY_ONCE, false, "\\Chii2\\Client", 77, content);
        storage.putPublish("TEST_CLIENT", message);
        assert storage.containPublish("TEST_CLIENT", message);
    }

    @Test
    public void pubrelStorageTest() {
        PubRelMessage message = new PubRelMessage(false, 198);
        storage.putPubRel("TEST_CLIENT", message);
        assert storage.containPubRel("TEST_CLIENT", message.getMessageID());
    }

    @Test
    public void subscribeStorageTest() {
        ByteBuf buf = Unpooled.buffer();
        List<SubscribeMessage.Topic> topics = new ArrayList<>();
        topics.add(new SubscribeMessage.Topic("\\Chii2\\Miao", MQTTMessage.QoSLevel.LEAST_ONCE));
        topics.add(new SubscribeMessage.Topic("\\Chii2\\Wang", MQTTMessage.QoSLevel.EXACTLY_ONCE));
        SubscribeMessage message = new SubscribeMessage(true, 3000, topics);
        storage.putSubscribe("TEST_CLIENT", message);
        assert storage.containSubscribe("TEST_CLIENT", message);
        List<String> topicNames = new ArrayList<>();
        for (SubscribeMessage.Topic topic :message.getTopics()) {
            topicNames.add(topic.getTopicName());
        }
        UnsubscribeMessage unsubscribeMessage = new UnsubscribeMessage(false, message.getMessageID(), topicNames);
        storage.removeSubscribe("TEST_CLIENT", unsubscribeMessage);
        assert !storage.containSubscribe("TEST_CLIENT", message);
    }
}
