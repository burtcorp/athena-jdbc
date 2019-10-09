package io.burt.athena.result.protobuf;

import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class VeryBasicProtobufParser {
    public List<Field> parse(InputStream input) throws IOException {
        return parse(IoUtils.toByteArray(input));
    }

    public List<Field> parse(byte[] bytes) {
        return parse(ByteBuffer.wrap(bytes));
    }

    public List<Field> parse(ByteBuffer buffer) {
        List<Field> fields = new LinkedList<>();
        while (buffer.hasRemaining()) {
            fields.add(readField(buffer));
        }
        return fields;
    }

    private Field readField(ByteBuffer buffer) {
        int x = Byte.toUnsignedInt(buffer.get());
        int fieldNumber = x >> 3;
        int fieldType = x & 7;
        switch (fieldType) {
            case 0:
                long value = readVarint(buffer);
                return new IntegerField(fieldNumber, value);
            case 2:
                byte[] contents = readLengthDelimited(buffer);
                return new BinaryField(fieldNumber, contents);
            default:
                throw new IllegalStateException(String.format("Unsupported field type: %d", fieldType));
        }
    }

    private byte[] readLengthDelimited(ByteBuffer buffer) {
        int size = Math.toIntExact(readVarint(buffer));
        byte[] contents = new byte[size];
        buffer.get(contents);
        return contents;
    }

    private long readVarint(ByteBuffer buffer) {
        int b = Byte.toUnsignedInt(buffer.get());
        int n = b & 0x7f;
        int i = 1;
        while ((b & 0x80) != 0) {
            b = Byte.toUnsignedInt(buffer.get());
            n |= ((b & 0x7f) << (i * 7));
            i++;
        }
        return n;
    }
}
