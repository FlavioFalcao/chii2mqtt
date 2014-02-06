package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventHandler;

/**
 * MQTT Message Journal Processor
 */
public class JournalProcessor implements EventHandler<MQTTEvent> {

    @Override
    public void onEvent(MQTTEvent event, long sequenceNumber, boolean endOfBatch) throws Exception {

    }
}
