package io.burt.athena.result.protobuf;

import java.util.Objects;

public class Field {
    private final int number;

    Field(int number) {
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
