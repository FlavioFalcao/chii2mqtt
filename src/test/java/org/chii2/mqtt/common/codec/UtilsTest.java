package org.chii2.mqtt.common.codec;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.EncoderException;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;

/**
 * MQTT Message Codec Utils Test
 */
public class UtilsTest {

    @Test
    public void decodeRemainingLengthTest() {
        ByteBuf byteBuf;
        byteBuf = Utils.encodeRemainingLength(64);
        assert 64 == Utils.decodeRemainingLength(byteBuf);
        byteBuf = Utils.encodeRemainingLength(127);
        assert 127 == Utils.decodeRemainingLength(byteBuf);
        byteBuf = Utils.encodeRemainingLength(8000);
        assert 8000 == Utils.decodeRemainingLength(byteBuf);
        byteBuf = Utils.encodeRemainingLength(16383);
        assert 16383 == Utils.decodeRemainingLength(byteBuf);
        byteBuf = Utils.encodeRemainingLength(150000);
        assert 150000 == Utils.decodeRemainingLength(byteBuf);
        byteBuf = Utils.encodeRemainingLength(2097151);
        assert 2097151 == Utils.decodeRemainingLength(byteBuf);
        byteBuf = Utils.encodeRemainingLength(50000000);
        assert 50000000 == Utils.decodeRemainingLength(byteBuf);
        byteBuf = Utils.encodeRemainingLength(268435455);
        assert 268435455 == Utils.decodeRemainingLength(byteBuf);
    }

    @Test
    public void encodeRemainingLengthTest() {
        ByteBuf byteBuf;
        byteBuf = Utils.encodeRemainingLength(64);
        assert byteBuf.readableBytes() == 1;
        byteBuf = Utils.encodeRemainingLength(127);
        assert byteBuf.readableBytes() == 1;
        byteBuf = Utils.encodeRemainingLength(8000);
        assert byteBuf.readableBytes() == 2;
        byteBuf = Utils.encodeRemainingLength(16383);
        assert byteBuf.readableBytes() == 2;
        byteBuf = Utils.encodeRemainingLength(150000);
        assert byteBuf.readableBytes() == 3;
        byteBuf = Utils.encodeRemainingLength(2097151);
        assert byteBuf.readableBytes() == 3;
        byteBuf = Utils.encodeRemainingLength(50000000);
        assert byteBuf.readableBytes() == 4;
        byteBuf = Utils.encodeRemainingLength(268435455);
        assert byteBuf.readableBytes() == 4;
    }

    @Test(expectedExceptions = EncoderException.class)
    public void encodeRemainingLengthWithExceptionTest() {
        Utils.encodeRemainingLength(268435456);
    }

    @Test
    public void decodeStringTest() throws UnsupportedEncodingException {
        ByteBuf bytebuf = Utils.encodeString("Test", "OTWP");
        assert "OTWP".equals(Utils.decodeString("Test", bytebuf));
    }

    @Test
    public void encodeStringTest() {
        ByteBuf bytebuf = Utils.encodeString("Test", "OTWP");
        bytebuf.skipBytes(2);
        assert bytebuf.readableBytes() == 4;
        assert bytebuf.readByte() == 0b1001111;
        assert bytebuf.readByte() == 0b1010100;
        assert bytebuf.readByte() == 0b1010111;
        assert bytebuf.readByte() == 0b1010000;
    }
}
