package org.chii2.mqtt.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.chii2.mqtt.common.codec.MQTTDecoder;
import org.chii2.mqtt.common.codec.MQTTEncoder;
import org.chii2.mqtt.server.disruptor.InboundDisruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MQTT Message Server
 */
public class MQTTServer {

    // The Logger
    private final Logger logger = LoggerFactory.getLogger(MQTTServer.class);
    // TCP/IP port 1883 is reserved with IANA for use with MQTT.
    private final int port;
    // TCP/IP port 8883 is reserved with IANA for use with MQTT over SSL.
    private final int sslPort;

    // Disruptor
    private final InboundDisruptor disruptor = new InboundDisruptor();
    // Configure the Netty server.
    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup();

    public MQTTServer(int port, int sslPort) {
        this.port = port;
        this.sslPort = sslPort;
    }

    /**
     * Start the MQTT Message Server
     */
    public void start() {
        try {
            // Init disruptor
            disruptor.start();
            // Init Netty server
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(
                                    new MQTTEncoder(),
                                    new MQTTDecoder(),
                                    new MQTTServerHandler(disruptor));
                        }
                    });

            // Start the server.
            ChannelFuture f = b.bind(port).sync();
            logger.info("{} has successfully started.", getServerName());
            // Server socket closed listener.
            f.channel().closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("{} has successfully stopped.", getServerName());
                }
            });
        } catch (Exception e) {
            logger.error("{} encountered fatal error: {}", getServerName(), ExceptionUtils.getMessage(e));
        }
    }

    /**
     * Stop the MQTT Message Server
     */
    public void stop() {
        // Shut down the disruptor
        disruptor.shutdown();
        // Shut down all event loops to terminate all threads.
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    /**
     * Get Server Name
     * This could be override to return vendor's name
     * @return Server Name
     */
    public String getServerName() {
        return "Chii2 MQTT Server";
    }
}
