package io.burt.athena.result.protobuf;

import java.util.Objects;

public class IntegerField extends Field {
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
