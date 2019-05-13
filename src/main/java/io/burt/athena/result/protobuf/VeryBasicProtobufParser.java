package io.burt.athena.result.protobuf;

import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

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
        int size = Byte.toUnsignedInt(buffer.get());
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

    public static class Field {
        private final int number;

        public Field(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field = (Field) o;
            return number == field.number;
        }

        @Override
        public int hashCode() {
            return Objects.hash(number);
        }
    }

    public static class BinaryField extends Field {
        private final byte[] contents;

        public BinaryField(int number, byte[] contents) {
            super(number);
            this.contents = contents;
        }

        public byte[] getContents() {
            return contents;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            BinaryField that = (BinaryField) o;
            return Arrays.equals(contents, that.contents);
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + Arrays.hashCode(contents);
            return result;
        }
    }

    public static class IntegerField extends Field {
        private final long value;

        public IntegerField(int number, long value) {
            super(number);
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            IntegerField that = (IntegerField) o;
            return value == that.value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), value);
        }
    }
}
