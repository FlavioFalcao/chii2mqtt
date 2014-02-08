package org.chii2.mqtt.server;

/**
 * Main Class
 */
public class Main {

    public static void main(String[] args) {
        MQTTServer server = new MQTTServer(1883, 8883);
        server.start();
    }
}
