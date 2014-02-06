package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.dsl.Disruptor;

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
    private static final int EVENT_PROCESSORS_NUM = 3;

    private final ExecutorService executor;
    private final Disruptor<MQTTEvent> disruptor;

    public InboundDisruptor() {
        executor = Executors.newFixedThreadPool(EVENT_PROCESSORS_NUM);
        disruptor = new Disruptor<>(MQTTEvent.factory, RING_BUFFER_SIZE, executor);
        disruptor.handleEventsWith(new JournalProcessor()).then(new LogicProcessor());
    }

    public void start() {
        disruptor.start();
    }

    public void shutdown() {
        disruptor.shutdown();
        executor.shutdown();
    }

    public void pushEvent(MQTTEventTranslator translator) {
        disruptor.publishEvent(translator);
    }
}
