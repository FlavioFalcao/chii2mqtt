package org.chii2.mqtt.common.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;

import java.io.UnsupportedEncodingException;

/**
 * MQTT Message Codec Utils
 */
public class Utils {

    // The protocol limits the number of bytes in the representation to a maximum of four.
    // This allows applications to send messages of up to 268 435 455 (256 MB).
    public static final int MAX_LENGTH_LIMIT = 268435455;

    /**
     * Decode the variable remaining length as defined in MQTT v3.1 specification
     * (section 2.1).
     *
     * @return the decoded length or -1 if needed more data to decode the length field.
     */
    public static int decodeRemainingLength(ByteBuf in) throws EncoderException {
        int multiplier = 1;
        int value = 0;
        byte digit;
        do {
            if (in.readableBytes() < 1) {
                return -1;
            }
            digit = in.readByte();
            value += (digit & 0x7F) * multiplier;
            multiplier *= 128;
        } while ((digit & 0x80) != 0);

        if (value > MAX_LENGTH_LIMIT || value < 0) {
            in.resetReaderIndex();
            throw new DecoderException("Remaining Length should in range 0.." + MAX_LENGTH_LIMIT + " found " + value);
        }

        return value;
    }

    /**
     * Encode the value in the format defined in specification as variable length
     * array.
     *
     * @throws EncoderException if the value is not in the specification bounds
     *                          [0..268435455].
     */
    public static ByteBuf encodeRemainingLength(int value) throws EncoderException {
        if (value > MAX_LENGTH_LIMIT || value < 0) {
            throw new EncoderException("Remaining Length should in range 0.." + MAX_LENGTH_LIMIT + " found " + value);
        }

        ByteBuf encoded = Unpooled.buffer(4);
        byte digit;
        do {
            digit = (byte) (value % 128);
            value = value / 128;
            // if there are more digits to encode, set the top bit of this digit
            if (value > 0) {
                digit = (byte) (digit | 0x80);
            }
            encoded.writeByte(digit);
        } while (value > 0);
        return encoded;
    }

    /**
     * Load a string from the given byte buffer, reading first the two bytes of length
     * and then the UTF-8 bytes of the string.
     *
     * @return the decoded string or null if need more data
     */
    public static String decodeString(String name, ByteBuf in) {
        int strLength = in.readUnsignedShort();
        byte[] strRaw = new byte[strLength];
        in.readBytes(strRaw);
        try {
            return new String(strRaw, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            in.resetReaderIndex();
            throw new DecoderException(name + "is not in UTF-8 format.");
        }
    }

    /**
     * Return the IoBuffer with string encoded as MSB, LSB and UTF-8 encoded
     * string content.
     */
    public static ByteBuf encodeString(String name, String value) {
        ByteBuf out = Unpooled.buffer(2);
        byte[] raw;
        try {
            raw = value.getBytes("UTF-8");
            // Note every Java platform has got UTF-8 encoding by default, so this
            // exception are never raised.
        } catch (UnsupportedEncodingException ex) {
            throw new EncoderException(name + "is not in UTF-8 format");
        }
        out.writeShort(raw.length);
        out.writeBytes(raw);
        return out;
    }
}
