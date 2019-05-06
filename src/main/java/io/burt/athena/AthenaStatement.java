package io.burt.athena;

import io.burt.athena.polling.PollingStrategy;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

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
import java.util.function.Supplier;

public class AthenaStatement implements Statement {
    private ConnectionConfiguration configuration;
    private AthenaAsyncClient athenaClient;
    private String queryExecutionId;
    private ResultSet currentResultSet;
    private Supplier<PollingStrategy> pollingStrategyFactory;
    private Function<String, Optional<String>> clientRequestTokenProvider;
    private boolean open;

    AthenaStatement(AthenaAsyncClient athenaClient, ConnectionConfiguration configuration, Supplier<PollingStrategy> pollingStrategyFactory) {
        this.athenaClient = athenaClient;
        this.configuration = configuration;
        this.pollingStrategyFactory = pollingStrategyFactory;
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
            Optional<String> clientRequestToken = clientRequestTokenProvider.apply(sql);
            StartQueryExecutionResponse startResponse = athenaClient.startQueryExecution(sqeb -> {
                sqeb.queryString(sql);
                sqeb.workGroup(configuration.workGroupName());
                sqeb.queryExecutionContext(ecb -> ecb.database(configuration.databaseName()));
                sqeb.resultConfiguration(rcb -> rcb.outputLocation(configuration.outputLocation()));
                clientRequestToken.ifPresent(sqeb::clientRequestToken);
            }).get(configuration.apiCallTimeout().toMillis(), TimeUnit.MILLISECONDS);
            queryExecutionId = startResponse.queryExecutionId();
            PollingStrategy pollingStrategy = pollingStrategyFactory.get();
            currentResultSet = pollingStrategy.pollUntilCompleted(() -> {
                GetQueryExecutionResponse statusResponse = athenaClient
                        .getQueryExecution(builder -> builder.queryExecutionId(queryExecutionId))
                        .get(configuration.apiCallTimeout().toMillis(), TimeUnit.MILLISECONDS);
                QueryExecutionState state = statusResponse.queryExecution().status().state();
                switch (state) {
                    case SUCCEEDED:
                        return Optional.of(new AthenaResultSet(athenaClient, configuration, this, statusResponse.queryExecution()));
                    case FAILED:
                    case CANCELLED:
                        throw new SQLException(statusResponse.queryExecution().status().stateChangeReason());
                    default:
                        return Optional.empty();
                }
            });
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
        athenaClient = null;
        open = false;
    }

    @Override
    public boolean isClosed() throws SQLException {
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
    public ResultSet getResultSet() throws SQLException {
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
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support auto generated keys");
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
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
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
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Athena does not support updates");
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
    public int getMaxFieldSize() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return (int) configuration.apiCallTimeout().toMillis() / 1000;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        configuration = configuration.withTimeout(Duration.ofSeconds(seconds));
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
    public void setCursorName(String name) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not implemented");
    }
}
