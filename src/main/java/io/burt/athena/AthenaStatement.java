package io.burt.athena;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class AthenaStatement implements Statement {
    private final AthenaAsyncClient athenaClient;

    private ConnectionConfiguration configuration;
    private String queryExecutionId;
    private ResultSet currentResultSet;
    private Function<String, Optional<String>> clientRequestTokenProvider;
    private boolean open;

    AthenaStatement(ConnectionConfiguration configuration) {
        this.configuration = configuration;
        this.athenaClient = configuration.athenaClient();
        this.queryExecutionId = null;
        this.currentResultSet = null;
        this.clientRequestTokenProvider = sql -> Optional.empty();
        this.open = true;
    }

    /**
     * Set a client request token provider for this statement.
     *
     * If query executions have the same client request token Athena can
     * immediately return the results instead of executing the request again and
     * again. This is a great way to save costs and improve performance.
     *
     * The client request token provider receives the SQL to be executed and is
     * expected to return an <code>Option</code> containing a token that
     * conforms to the requirements of the <code>ClientRequestToken</code>
     * property of the <code>StartQueryExecutionRequest</code>, or
     * <code>Option.empty()</code> when the request should not have a client
     * request token.
     *
     * @param provider the function that produces the client request token given
     *                 the SQL to be executed
     */
    public void setClientRequestTokenProvider(Function<String, Optional<String>> provider) {
        if (provider == null) {
            clientRequestTokenProvider = sql -> Optional.empty();
        } else {
            clientRequestTokenProvider = provider;
        }
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        execute(sql);
        return getResultSet();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
        try {
            queryExecutionId = startQueryExecution(sql);
            currentResultSet = configuration.pollingStrategy().pollUntilCompleted(this::poll);
            return currentResultSet != null;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        } catch (TimeoutException ie) {
            throw new SQLTimeoutException(ie);
        } catch (ExecutionException ee) {
            throw new SQLException(ee);
        }
    }

    private String startQueryExecution(String sql) throws InterruptedException, ExecutionException, TimeoutException {
        return athenaClient
                .startQueryExecution(b -> {
                    b.queryString(sql);
                    b.workGroup(configuration.workGroupName());
                    b.queryExecutionContext(bb -> bb.database(configuration.databaseName()));
                    b.resultConfiguration(bb -> bb.outputLocation(configuration.outputLocation()));
                    clientRequestTokenProvider.apply(sql).ifPresent(b::clientRequestToken);
                })
                .get(configuration.apiCallTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .queryExecutionId();
    }

    private Optional<ResultSet> poll() throws SQLException, InterruptedException, ExecutionException, TimeoutException {
        QueryExecution queryExecution = athenaClient
                .getQueryExecution(b -> b.queryExecutionId(queryExecutionId))
                .get(configuration.apiCallTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .queryExecution();
        switch (queryExecution.status().state()) {
            case SUCCEEDED:
                return Optional.of(createResultSet(queryExecution));
            case FAILED:
            case CANCELLED:
                throw new SQLException(queryExecution.status().stateChangeReason());
            default:
                return Optional.empty();
        }
    }

    private ResultSet createResultSet(QueryExecution queryExecution) {
        return new AthenaResultSet(
                configuration.createResult(queryExecution),
                this
        );
    }

    private void checkClosed() throws SQLException {
        if (!open) {
            throw new SQLException("Statement is closed");
        }
    }

    @Override
    public void close() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close();
        }
        open = false;
    }

    @Override
    public boolean isClosed() {
        return !open;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        if (queryExecutionId == null) {
            throw new SQLException("Cannot cancel a statement before it has started");
        } else if (getResultSet() != null) {
            throw new SQLException("Cannot cancel an completed statement");
        } else {
            athenaClient.stopQueryExecution(b -> b.queryExecutionId(queryExecutionId));
        }
    }

    @Override
    public ResultSet getResultSet() {
        return currentResultSet;
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
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == NO_GENERATED_KEYS) {
            return execute(sql);
        } else {
            throw new SQLFeatureNotSupportedException("Athena does not support auto generated keys");
        }
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support auto generated keys");
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support auto generated keys");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        execute(sql);
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == NO_GENERATED_KEYS) {
            return executeUpdate(sql);
        } else {
            throw new SQLFeatureNotSupportedException("Athena does not support updates");
        }
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        execute(sql);
        return 0;
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == NO_GENERATED_KEYS) {
            return executeLargeUpdate(sql);
        } else {
            throw new SQLFeatureNotSupportedException("Athena does not support updates");
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
    }

    @Override
    public int getMaxFieldSize() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setMaxFieldSize(int max) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxRows() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setMaxRows(int max) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getQueryTimeout() {
        return (int) configuration.apiCallTimeout().toMillis() / 1000;
    }

    @Override
    public void setQueryTimeout(int seconds) {
        configuration = configuration.withTimeout(Duration.ofSeconds(seconds));
    }

    @Override
    public SQLWarning getWarnings() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void clearWarnings() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setCursorName(String name) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getUpdateCount() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean getMoreResults() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setFetchDirection(int direction) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getFetchDirection() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setFetchSize(int rows) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getFetchSize() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getResultSetConcurrency() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getResultSetType() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void addBatch(String sql) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void clearBatch() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int[] executeBatch() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Connection getConnection() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean getMoreResults(int current) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getGeneratedKeys() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getResultSetHoldability() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setPoolable(boolean poolable) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isPoolable() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void closeOnCompletion() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isCloseOnCompletion() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
