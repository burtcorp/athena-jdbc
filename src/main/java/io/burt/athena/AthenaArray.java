package io.burt.athena;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;

public class AthenaArray implements Array {
    private final String[] elements;

    public AthenaArray(String[] elements) {
        this.elements = elements;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return "VARCHAR";
    }

    @Override
    public int getBaseType() throws SQLException {
        return Types.VARCHAR;
    }

    @Override
    public Object getArray() throws SQLException {
        return elements;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting the elements of Array to other types is not supported");
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return Arrays.copyOfRange(elements, (int) index, (int) index + count);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting the elements of Array to other types is not supported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting Array to ResultSet is not supported");
    }

    @Override
    public void free() throws SQLException {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AthenaArray that = (AthenaArray) o;
        return Arrays.equals(elements, that.elements);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(elements);
    }

    @Override
    public String toString() {
        return String.join(", ", elements);
    }
}
