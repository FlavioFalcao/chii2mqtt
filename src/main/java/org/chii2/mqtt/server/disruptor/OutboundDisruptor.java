package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import org.chii2.mqtt.server.storage.HawtDBStorage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Outbound Disruptor
 * Receive Outbound event from InboundProcessor and handled by OutboundProcessor.
 * Refer to LMX Disruptor web site for more information.
 */
public class OutboundDisruptor {

    // Ring Buffer Size
    private static final int RING_BUFFER_SIZE = 1024;
    // Processor threads count
    private static final int EVENT_PROCESSORS_NUM = 2;

    private final ExecutorService executor;
    private final Disruptor<OutboundMQTTEvent> disruptor;

    public OutboundDisruptor(long interval, HawtDBStorage storage) {
        executor = Executors.newFixedThreadPool(EVENT_PROCESSORS_NUM);
        disruptor = new Disruptor<>(OutboundMQTTEvent.factory, RING_BUFFER_SIZE, executor);
        disruptor.handleEventsWith(new OutboundProcessor(storage)).then(new OutboundResendProcessor(interval, storage, this));
    }

    public void start() {
        disruptor.start();
    }

    public void shutdown() {
        disruptor.shutdown();
        executor.shutdown();
    }

    public void pushEvent(OutboundMQTTEventTranslator translator) {
        disruptor.publishEvent(translator);
    }
}
