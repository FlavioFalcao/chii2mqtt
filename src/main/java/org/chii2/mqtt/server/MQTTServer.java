package org.chii2.mqtt.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.chii2.mqtt.common.codec.MQTTDecoder;
import org.chii2.mqtt.common.codec.MQTTEncoder;
import org.chii2.mqtt.server.disruptor.InboundDisruptor;

/**
 * MQTT Message Server
 */
public class MQTTServer {

    // Sever Port
    private final int port;

    public MQTTServer(int port) {
        this.port = port;
    }

    public void start() {
        // Configure the server.
        final EventLoopGroup bossGroup = new NioEventLoopGroup();
        final EventLoopGroup workerGroup = new NioEventLoopGroup();
        final InboundDisruptor disruptor = new InboundDisruptor();

        try {
            disruptor.start();

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
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

            // Wait until the server socket is closed.
            f.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            // Ignore
        } finally {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            disruptor.shutdown();
        }
    }
}
