package org.chii2.mqtt.common.message;

/**
 * CONNECT Message - Client requests a connection to a server
 * <p/>
 * When a TCP/IP socket connection is established from a client to a server, a protocol
 * level session must be created using a CONNECT flow.
 */
public class ConnectMessage extends MQTTMessage {

    // Protocol Name (Should be MQIsdp)
    protected String protocolName;
    // Protocol Version (Should be 0x03)
    protected byte protocolVersion;
    // Connect flags
    protected boolean cleanSession;
    protected boolean willFlag;
    protected QoSLevel willQoS;
    protected boolean willRetain;
    protected boolean passwordFlag;
    protected boolean userNameFlag;
    protected int keepAlive;

    // Payload (in order if present)
    protected String clientID;
    protected String willTopic;
    protected String willMessage;
    protected String userName;
    protected String password;

    /**
     * INTERNAL USE ONLY
     */
    public ConnectMessage() {
        // Set Message Type
        this.messageType = MessageType.CONNECT;
        // Set Protocol Name
        this.protocolName = PROTOCOL_NAME;
        // Set Protocol Version
        // TODO: Current MQTT Version is V3.1, This may change in future
        this.protocolVersion = PROTOCOL_VERSION;
    }

    public ConnectMessage(boolean userNameFlag, boolean passwordFlag, boolean willRetain, QoSLevel willQoS, boolean willFlag, boolean cleanSession, int keepAlive,
                          String clientID, String willTopic, String willMessage, String userName, String password) {
        this();
        this.userNameFlag = userNameFlag;
        this.passwordFlag = passwordFlag;
        this.willRetain = willRetain;
        this.willQoS = willQoS;
        this.willFlag = willFlag;
        this.cleanSession = cleanSession;
        this.keepAlive = keepAlive;
        this.clientID = clientID;
        this.willTopic = willTopic;
        this.willMessage = willMessage;
        this.userName = userName;
        this.password = password;
        this.remainingLength = calculateRemainingLength();
    }

    @Override
    protected int calculateRemainingLength() {
        int length = 12;
        length = length + 2 + clientID.getBytes().length;
        if (willFlag) {
            length = length + 2 + willTopic.getBytes().length;
            length = length + 2 + willMessage.getBytes().length;
        }
        if (userNameFlag) {
            length = length + 2 + userName.getBytes().length;
            if (passwordFlag) {
                length = length + 2 + password.getBytes().length;
            }
        }
        return length;
    }

    public boolean isAcceptableProtocolVersion() {
        // TODO: Current MQTT Version is V3.1, This may change in future
        return protocolVersion == PROTOCOL_VERSION;
    }

    public String getProtocolName() {
        return protocolName;
    }

    public void setProtocolName(String protocolName) {
        this.protocolName = protocolName;
    }

    public byte getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(byte protocolVersion) {
        this.protocolVersion = protocolVersion;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public boolean isWillFlag() {
        return willFlag;
    }

    public void setWillFlag(boolean willFlag) {
        this.willFlag = willFlag;
    }

    public QoSLevel getWillQoS() {
        return willQoS;
    }

    public void setWillQoS(QoSLevel willQoS) {
        this.willQoS = willQoS;
    }

    public boolean isWillRetain() {
        return willRetain;
    }

    public void setWillRetain(boolean willRetain) {
        this.willRetain = willRetain;
    }

    public boolean isPasswordFlag() {
        return passwordFlag;
    }

    public void setPasswordFlag(boolean passwordFlag) {
        this.passwordFlag = passwordFlag;
    }

    public boolean isUserNameFlag() {
        return userNameFlag;
    }

    public void setUserNameFlag(boolean userNameFlag) {
        this.userNameFlag = userNameFlag;
    }

    public int getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(int keepAlive) {
        this.keepAlive = keepAlive;
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public String getWillMessage() {
        return willMessage;
    }

    public void setWillMessage(String willMessage) {
        this.willMessage = willMessage;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
