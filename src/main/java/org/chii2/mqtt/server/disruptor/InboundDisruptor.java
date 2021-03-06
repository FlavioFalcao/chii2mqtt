package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import org.chii2.mqtt.server.storage.HawtDBStorage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Inbound Disruptor
 * Receives MQTT Message from Netty handler and route to Processors.
 * Refer to LMX Disruptor web site for more information.
 */
public class InboundDisruptor {

    // Ring Buffer Size
    private static final int RING_BUFFER_SIZE = 1024;
    // Processor threads count
    private static final int EVENT_PROCESSORS_NUM = 1;

    private final ExecutorService executor;
    private final Disruptor<InboundMQTTEvent> disruptor;

    public InboundDisruptor(HawtDBStorage storage) {
        executor = Executors.newFixedThreadPool(EVENT_PROCESSORS_NUM);
        disruptor = new Disruptor<>(InboundMQTTEvent.factory, RING_BUFFER_SIZE, executor);
        disruptor.handleEventsWith(new InboundProcessor(storage));
    }

    public void start() {
        disruptor.start();
    }

    public void shutdown() {
        disruptor.shutdown();
        executor.shutdown();
    }

    public void pushEvent(InboundMQTTEventTranslator translator) {
        disruptor.publishEvent(translator);
    }
}
