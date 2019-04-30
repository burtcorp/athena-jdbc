package io.burt.athena;

import io.burt.athena.result.PreloadingStandardResult;
import io.burt.athena.result.Result;
import io.burt.athena.result.ResultPosition;
import io.burt.athena.result.StandardResult;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Pattern;

public class AthenaResultSet implements ResultSet {
    private final String queryExecutionId;
    private AthenaStatement statement;
    private boolean open;
    private Result result;
    private boolean lastWasNull;

    AthenaResultSet(AthenaAsyncClient athenaClient, ConnectionConfiguration configuration, AthenaStatement statement, String queryExecutionId) {
        this.statement = statement;
        this.queryExecutionId = queryExecutionId;
        this.open = true;
        this.result = new PreloadingStandardResult(athenaClient, queryExecutionId, StandardResult.MAX_FETCH_SIZE, configuration.apiCallTimeout());
        this.lastWasNull = false;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    public String queryExecutionId() {
        return queryExecutionId;
    }

    private void checkClosed() throws SQLException {
        if (!open) {
            throw new SQLException("Result set is closed");
        }
    }

    private void checkPosition(int columnIndex) throws SQLException {
        checkVerticalPosition();
        checkHorizontalPosition(columnIndex);
    }

    private void checkPosition(String columnLabel) throws SQLException {
        checkVerticalPosition();
        checkHorizontalPosition(findColumn(columnLabel));
    }

    private void checkVerticalPosition() throws SQLException {
        if (isBeforeFirst()) {
            throw new SQLException("Cannot read from a result set positioned before the first row");
        } else if (isAfterLast()) {
            throw new SQLException("Cannot read from a result set positioned after the last row");
        }
    }

    private void checkHorizontalPosition(int columnIndex) throws SQLException {
        int columnCount = getMetaData().getColumnCount();
        if (columnIndex < 1) {
            throw new SQLException(String.format("Invalid column index %d", columnIndex));
        } else if (columnIndex > columnCount) {
            throw new SQLException(String.format("Column index out of bounds (%d > %d)", columnIndex, columnCount));
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        return result.next();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return result.metaData();
    }

    @Override
    public void close() throws SQLException {
        statement = null;
        open = false;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !open;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return result.position() == ResultPosition.BEFORE_FIRST;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return result.position() == ResultPosition.AFTER_LAST;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return result.position() == ResultPosition.FIRST;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return result.position() == ResultPosition.LAST;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        if (isBeforeFirst() || isAfterLast()) {
            return 0;
        } else {
            return result.rowNumber();
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Result set movements other than forward are not supported");
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw new SQLException(String.format("Fetch size cannot be negative (got %d)", rows));
        } else {
            result.updateFetchSize(rows);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return result.fetchSize();
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        ResultSetMetaData metaData = getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            if (metaData.getColumnLabel(i).equals(columnLabel)) {
                return i;
            }
        }
        throw new SQLDataException(String.format("Result set does not contain any column with label \"%s\"", columnLabel));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format("%s is not a wrapper for %s", this.getClass().getName(), iface.getName()));
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Holdability is not defined for Athena");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Holdability is not defined for Athena");
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastWasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        checkPosition(columnIndex);
        String value = result.stringValue(columnIndex);
        lastWasNull = value == null;
        return value;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        checkClosed();
        checkPosition(columnLabel);
        return getString(findColumn(columnLabel));
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    private Boolean convertToBoolean(String str) {
        return !(str == null || str.equals("0") || str.equalsIgnoreCase("false"));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return convertToBoolean(getString(columnIndex));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return convertToBoolean(getString(columnLabel));
    }

    private byte convertToByte(String str) throws SQLException {
        if (str == null) {
            return 0;
        } else {
            try {
                return Byte.valueOf(str);
            } catch (NumberFormatException nfe) {
                throw new SQLDataException(String.format("Cannot convert \"%s\" to byte", str), nfe);
            }
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return convertToByte(getString(columnIndex));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return convertToByte(getString(columnLabel));
    }

    private short convertToShort(String str) throws SQLException {
        if (str == null) {
            return 0;
        } else {
            try {
                return Short.valueOf(str);
            } catch (NumberFormatException nfe) {
                throw new SQLDataException(String.format("Cannot convert \"%s\" to short", str), nfe);
            }
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return convertToShort(getString(columnIndex));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return convertToShort(getString(columnLabel));
    }

    private int convertToInteger(String str) throws SQLException {
        if (str == null) {
            return 0;
        } else {
            try {
                return Integer.valueOf(str);
            } catch (NumberFormatException nfe) {
                throw new SQLDataException(String.format("Cannot convert \"%s\" to integer", str), nfe);
            }
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return convertToInteger(getString(columnIndex));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return convertToInteger(getString(columnLabel));
    }

    private long convertToLong(String str) throws SQLException {
        if (str == null) {
            return 0;
        } else {
            try {
                return Long.valueOf(str);
            } catch (NumberFormatException nfe) {
                throw new SQLDataException(String.format("Cannot convert \"%s\" to long", str), nfe);
            }
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return convertToLong(getString(columnIndex));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return convertToLong(getString(columnLabel));
    }

    private float convertToFloat(String str) throws SQLException {
        if (str == null) {
            return 0f;
        } else {
            try {
                Float f = Float.valueOf(str);
                if (f.isInfinite()) {
                    throw new SQLDataException(String.format("Cannot convert \"%s\" to float", str));
                } else {
                    return f;
                }
            } catch (NumberFormatException nfe) {
                throw new SQLDataException(String.format("Cannot convert \"%s\" to float", str), nfe);
            }
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return convertToFloat(getString(columnIndex));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return convertToFloat(getString(columnLabel));
    }

    private double convertToDouble(String str) throws SQLException {
        if (str == null) {
            return 0d;
        } else {
            try {
                Double d = Double.valueOf(str);
                if (d.isInfinite()) {
                    throw new SQLDataException(String.format("Cannot convert \"%s\" to double", str));
                } else {
                    return d;
                }
            } catch (NumberFormatException nfe) {
                throw new SQLDataException(String.format("Cannot convert \"%s\" to double", str), nfe);
            }
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return convertToDouble(getString(columnIndex));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return convertToDouble(getString(columnLabel));
    }

    private BigDecimal convertToBigDecimal(String str) throws SQLException {
        if (str == null) {
            return null;
        } else {
            try {
                return new BigDecimal(str);
            } catch (NumberFormatException nfe) {
                throw new SQLDataException(String.format("Could not convert \"%s\" to BigDecimal", str), nfe);
            }
        }
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return convertToBigDecimal(getString(columnIndex));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return convertToBigDecimal(getString(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return convertToBigDecimal(getString(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return convertToBigDecimal(getString(columnLabel));
    }

    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final Pattern SPACE = Pattern.compile(" ");

    private byte[] convertToBytes(String str, boolean isVarbinary) {
        if (str == null) {
            return null;
        } else if (isVarbinary) {
            String[] hextets = SPACE.split(str);
            byte[] bytes = new byte[hextets.length];
            for (int i = 0; i < hextets.length; i++) {
                bytes[i] = Byte.valueOf(hextets[i], 16);
            }
            return bytes;
        } else {
            return str.getBytes(UTF_8);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return convertToBytes(getString(columnIndex), getMetaData().getColumnTypeName(columnIndex).equalsIgnoreCase("varbinary"));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    private Date convertToDate(String str) throws SQLException {
        if (str == null) {
            return null;
        } else {
            try {
                LocalDate date = DateTimeFormatter.ISO_DATE.parse(str, TemporalQueries.localDate());
                return Date.valueOf(date);
            } catch (DateTimeParseException dtpe) {
                throw new SQLDataException(String.format("Could not convert \"%s\" to Date", str), dtpe);
            }
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return convertToDate(getString(columnIndex));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return convertToDate(getString(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date/time retrieval relative to a Calendar not supported");
    }

    @Override
    public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date/time retrieval relative to a Calendar not supported");
    }

    private static final DateTimeFormatter ATHENA_TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS[ VV][ zzzz]");

    private Time convertToTime(String str) throws SQLException {
        if (str == null) {
            return null;
        } else {
            try {
                LocalTime time = ATHENA_TIME_FORMAT.parse(str, TemporalQueries.localTime());
                ZoneId zone = ATHENA_TIME_FORMAT.parse(str, TemporalQueries.zone());
                if (zone != null) {
                    time = LocalDateTime
                            .of(LocalDate.now(), time)
                            .atZone(zone)
                            .withZoneSameInstant(ZoneId.systemDefault())
                            .toLocalTime();
                }
                return Time.valueOf(time);
            } catch (DateTimeParseException dtpe) {
                throw new SQLDataException(String.format("Could not convert \"%s\" to Time", str), dtpe);
            }
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return convertToTime(getString(columnIndex));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date/time retrieval relative to a Calendar not supported");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date/time retrieval relative to a Calendar not supported");
    }

    private static final DateTimeFormatter ATHENA_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS[ VV][ zzzz]");

    private Timestamp convertToTimestamp(String str) throws SQLException {
        if (str == null) {
            return null;
        } else {
            try {
                TemporalAccessor parsedTimestamp = ATHENA_TIMESTAMP_FORMAT.parseBest(str, ZonedDateTime::from, LocalDateTime::from);
                ZonedDateTime zonedTimestamp;
                if (parsedTimestamp instanceof ZonedDateTime) {
                    zonedTimestamp = (ZonedDateTime) parsedTimestamp;
                } else {
                    zonedTimestamp = ((LocalDateTime) parsedTimestamp).atZone(ZoneId.systemDefault());
                }
                return new Timestamp(zonedTimestamp.toInstant().toEpochMilli());
            } catch (DateTimeParseException e) {
                throw new SQLDataException(String.format("Could not convert \"%s\" to Timestamp", str), e);
            }
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return convertToTimestamp(getString(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date/time retrieval relative to a Calendar not supported");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date/time retrieval relative to a Calendar not supported");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        switch (getMetaData().getColumnType(columnIndex)) {
            case Types.TINYINT:
                byte b = getByte(columnIndex);
                return wasNull() ? null : b;
            case Types.SMALLINT:
                short s = getShort(columnIndex);
                return wasNull() ? null : s;
            case Types.INTEGER:
                int i = getInt(columnIndex);
                return wasNull() ? null : i;
            case Types.BIGINT:
                long l = getLong(columnIndex);
                return wasNull() ? null : l;
            case Types.FLOAT:
                float f = getFloat(columnIndex);
                return wasNull() ? null : f;
            case Types.DOUBLE:
                double d = getDouble(columnIndex);
                return wasNull() ? null : d;
            case Types.DECIMAL:
                return getBigDecimal(columnIndex);
            case Types.BOOLEAN:
                boolean o = getBoolean(columnIndex);
                return wasNull() ? null : o;
            case Types.VARBINARY:
                return getBytes(columnIndex);
            case Types.DATE:
                return getDate(columnIndex);
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return getTime(columnIndex);
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return getTimestamp(columnIndex);
            case Types.ARRAY:
                return getArray(columnIndex);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.STRUCT:
            case Types.OTHER:
            default:
                return getString(columnIndex);
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting to custom types is not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting to custom types is not supported");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting to custom types is not supported");
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("Converting to custom types is not supported");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    private Array convertToArray(String str) throws SQLException {
        if (str == null) {
            return null;
        } else if (str.startsWith("[") && str.endsWith("]")) {
            String[] elements = str.substring(1, str.length() - 1).split(", ");
            return new AthenaArray(elements);
        } else {
            throw new SQLDataException(String.format("Could not convert \"%s\" to an array", str));
        }
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return convertToArray(getString(columnIndex));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return convertToArray(getString(columnLabel));
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    private void movementsNotSupported() throws SQLException {
        throw new SQLFeatureNotSupportedException("Result set movements other than forward are not supported");
    }

    @Override
    public void beforeFirst() throws SQLException {
        movementsNotSupported();
    }

    @Override
    public void afterLast() throws SQLException {
        movementsNotSupported();
    }

    @Override
    public boolean first() throws SQLException {
        movementsNotSupported();
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        movementsNotSupported();
        return false;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (row < 1) {
            throw new SQLException(String.format("Invalid row number %d", row));
        } else if (row < result.rowNumber()) {
            throw new SQLException(String.format("Only forward movement is supported (cannot go back to %d from %d)", row, result.rowNumber()));
        } else {
            boolean status = false;
            while (result.rowNumber() < row) {
                status = next();
            }
            return status;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (rows < 1) {
            throw new SQLException("Only forward relative movement is supported");
        }
        boolean status = false;
        for (int i = 0; i < rows; i++) {
            status = next();
        }
        return status;
    }

    @Override
    public boolean previous() throws SQLException {
        movementsNotSupported();
        return false;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkClosed();
        return false;
    }

    private void mutationsNotSupported() throws SQLException {
        throw new SQLFeatureNotSupportedException("Mutation of result sets is not supported");
    }

    @Override
    public void insertRow() throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateRow() throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void deleteRow() throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void refreshRow() throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        mutationsNotSupported();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        mutationsNotSupported();
    }
}
