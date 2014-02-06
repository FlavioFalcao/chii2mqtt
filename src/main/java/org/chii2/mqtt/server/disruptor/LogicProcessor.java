package org.chii2.mqtt.server.disruptor;

import com.lmax.disruptor.EventHandler;
import io.netty.channel.ChannelHandlerContext;
import org.chii2.mqtt.common.message.ConnAckMessage;
import org.chii2.mqtt.common.message.ConnectMessage;
import org.chii2.mqtt.common.message.MQTTMessage;

/**
 * MQTT Message Core Logic Processor
 */
public class LogicProcessor implements EventHandler<MQTTEvent> {

    @Override
    public void onEvent(MQTTEvent event, long sequenceNumber, boolean endOfBatch) throws Exception {
        MQTTMessage message = event.getMQTTMessage();
        if (message instanceof ConnectMessage) {
            onConnect(event, sequenceNumber, endOfBatch);
        }
    }

    protected void onConnect(MQTTEvent event, long sequenceNumber, boolean endOfBatch) {
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
        context.writeAndFlush(connAckMessage);
    }
}
