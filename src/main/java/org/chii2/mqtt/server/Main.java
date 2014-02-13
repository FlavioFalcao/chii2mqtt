package org.chii2.mqtt.server;

import java.io.File;

/**
 * Main Class
 */
public class Main {

    public static void main(String[] args) {
        String storagePath = System.getProperty("user.home") + File.separator + "chii2mqtt.db";
        MQTTServer server = new MQTTServer(1883, 8883, storagePath);
        server.start();
    }
}
