package io.burt.athena.result.protobuf;

import java.util.Arrays;

public class BinaryField extends Field {
    private final byte[] contents;

    BinaryField(int number, byte[] contents) {
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
