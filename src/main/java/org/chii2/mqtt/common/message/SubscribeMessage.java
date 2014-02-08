package org.chii2.mqtt.common.message;

import java.util.ArrayList;
import java.util.List;

/**
 * SUBSCRIBE Message - Subscribe to named topics
 * <p/>
 * The SUBSCRIBE message allows a client to register an interest in one or more topic
 * names with the server. Messages published to these topics are delivered from the
 * server to the client as PUBLISH messages. The SUBSCRIBE message also specifies the
 * QoS level at which the subscriber wants to receive published messages.
 */
public class SubscribeMessage extends MQTTMessage {

    /**
     * Represent the topic client subscribe to
     */
    public static class Topic {

        // Topic
        protected String topicName;
        // Qos Level
        protected QoSLevel qosLevel;

        public Topic(String topicName, QoSLevel qosLevel) {
            this.topicName = topicName;
            this.qosLevel = qosLevel;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public QoSLevel getQosLevel() {
            return qosLevel;
        }

        public void setQosLevel(QoSLevel qosLevel) {
            this.qosLevel = qosLevel;
        }
    }

    // Message ID
    protected int messageID;
    // Payload, Topics
    protected List<Topic> topics = new ArrayList<>();

    /**
     * INTERNAL USE ONLY
     */
    public SubscribeMessage() {
        // Set Message Type
        this.messageType = MessageType.SUBSCRIBE;
        // SUBSCRIBE messages use QoS level 1 to acknowledge multiple subscription requests.
        this.qosLevel = QoSLevel.LEAST_ONCE;
    }

    public SubscribeMessage(boolean dupFlag, int messageID, List<Topic> topics) {
        this();
        this.dupFlag = dupFlag;
        this.messageID = messageID;
        this.topics = topics;
        this.remainingLength = calculateRemainingLength();
    }

    @Override
    protected int calculateRemainingLength() {
        int length = 2;
        for (Topic topic : topics) {
            length = length + 2 + topic.getTopicName().getBytes().length + 1;
        }
        return length;
    }

    public int getMessageID() {
        return messageID;
    }

    public void setMessageID(int messageID) {
        this.messageID = messageID;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;
    }

    public void addTopic(Topic topic) {
        this.topics.add(topic);
    }

    public void removeTopic(Topic topic) {
        this.topics.remove(topic);
    }
}
