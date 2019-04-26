package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;

import java.sql.SQLException;

public interface Result {
    int fetchSize() throws SQLException;

    void updateFetchSize(int newFetchSize) throws SQLException;

    AthenaResultSetMetaData metaData() throws SQLException;

    int rowNumber() throws SQLException;

    boolean next() throws SQLException;

    String stringValue(int columnIndex) throws SQLException;

    ResultPosition position() throws SQLException;
}
