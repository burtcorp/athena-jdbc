package io.burt.athena.result;

import io.burt.athena.AthenaResultSetMetaData;

import java.sql.SQLException;

public interface Result {
    int getFetchSize() throws SQLException;

    void setFetchSize(int newFetchSize) throws SQLException;

    AthenaResultSetMetaData getMetaData() throws SQLException;

    int getRowNumber() throws SQLException;

    boolean next() throws SQLException;

    String getString(int columnIndex) throws SQLException;

    ResultPosition getPosition() throws SQLException;
}
