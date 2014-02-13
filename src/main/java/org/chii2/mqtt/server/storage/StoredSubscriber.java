package org.chii2.mqtt.server.storage;

import java.io.Serializable;

/**
 * MQTT Subscriber in Storage
 */
public class StoredSubscriber implements Serializable {

    private String subscriberID;
    private int qosLevel;

    public StoredSubscriber(String subscriberID, int qosLevel) {
        this.subscriberID = subscriberID;
        this.qosLevel = qosLevel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoredSubscriber that = (StoredSubscriber) o;

        if (!subscriberID.equals(that.subscriberID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return subscriberID.hashCode();
    }

    public String getSubscriberID() {
        return subscriberID;
    }

    public void setSubscriberID(String subscriberID) {
        this.subscriberID = subscriberID;
    }

    public int getQosLevel() {
        return qosLevel;
    }

    public void setQosLevel(int qosLevel) {
        this.qosLevel = qosLevel;
    }
}
