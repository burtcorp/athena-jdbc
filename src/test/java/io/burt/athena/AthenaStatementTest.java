package io.burt.athena;

import io.burt.athena.configuration.ConnectionConfiguration;
import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.result.Result;
import io.burt.athena.support.ConfigurableConnectionConfiguration;
import io.burt.athena.support.QueryExecutionHelper;
import io.burt.athena.support.TestClock;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.InternalServerException;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StopQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.TooManyRequestsException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class AthenaStatementTest {
    private Result result;
    private QueryExecutionHelper queryExecutionHelper;
    private AthenaStatement statement;
    private PollingStrategy pollingStrategy;
    private QueryExecution resultFactoryQueryExecution;
    private TestClock clock;

    @BeforeEach
    void setUpStatement() {
        result = mock(Result.class);
        pollingStrategy = createPollingStrategy();
        clock = new TestClock();
        queryExecutionHelper = new QueryExecutionHelper(clock);
        statement = new AthenaStatement(createConfiguration(), clock);
    }

    PollingStrategy createPollingStrategy() {
        return (callback, deadline) -> {
            while (true) {
                Optional<ResultSet> rs = callback.poll(deadline);
                if (rs.isPresent()) {
                    return rs.get();
                }
            }
        };
    }

    ConnectionConfiguration createConfiguration() {
        return new ConfigurableConnectionConfiguration(
                "test_db",
                "test_wg",
                "s3://test/location",
                Duration.ofSeconds(60),
                Duration.ofSeconds(60),
                () -> queryExecutionHelper,
                () -> null,
                () -> pollingStrategy,
                (q) -> {
                    resultFactoryQueryExecution = q;
                    return result;
                }
        );
    }

    class SharedExecuteSetup {
        @BeforeEach
        void setUpStartQueryExecution() {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
        }

        @BeforeEach
        void setUpGetQueryExecution() {
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.QUEUED);
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.RUNNING);
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
        }
    }

    abstract class SharedExecuteTests<T> extends SharedExecuteSetup {
        protected abstract T execute() throws SQLException;

        StartQueryExecutionRequest executionRequest() {
            List<StartQueryExecutionRequest> requests = queryExecutionHelper.startQueryRequests();
            return requests.get(requests.size() - 1);
        }

        @Test
        void startsQueryExecution() throws Exception {
            execute();
            assertEquals("SELECT 1", executionRequest().queryString());
        }

        @Test
        void executesInTheConfiguredDatabase() throws Exception {
            execute();
            assertEquals("test_db", executionRequest().queryExecutionContext().database());
        }

        @Test
        void executesInTheConfiguredWorkGroup() throws Exception {
            execute();
            assertEquals("test_wg", executionRequest().workGroup());
        }

        @Test
        void usesTheConfiguredOutputLocation() throws Exception {
            execute();
            assertEquals("s3://test/location", executionRequest().resultConfiguration().outputLocation());
        }

        @Test
        void pollsForStatus() throws Exception {
            execute();
            List<GetQueryExecutionRequest> pollRequests = queryExecutionHelper.getQueryExecutionRequests();
            for (GetQueryExecutionRequest request : pollRequests) {
                assertEquals("Q1234", request.queryExecutionId());
            }
        }

        @Test
        void pollsUntilSucceeded() throws Exception {
            execute();
            assertEquals(3, queryExecutionHelper.getQueryExecutionRequests().size());
        }

        @Test
        void throwsWhenStartQueryExecutionThrows() {
            queryExecutionHelper.queueStartQueryExecutionException(InternalServerException.builder().message("b0rk").build());
            Exception e = assertThrows(SQLException.class, this::execute);
            assertTrue(e.getCause() instanceof InternalServerException);
        }

        @Test
        void throwsWhenGetQueryExecutionThrows() {
            queryExecutionHelper.queueStartQueryExecutionException(TooManyRequestsException.builder().message("b0rk").build());
            Exception e = assertThrows(SQLException.class, this::execute);
            assertTrue(e.getCause() instanceof TooManyRequestsException);
        }

        @Test
        void throwsOnFailure() {
            queryExecutionHelper.clearGetQueryExecutionResponseQueue();
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.RUNNING);
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.FAILED, "Teh bork");
            SQLException e = assertThrows(SQLException.class, this::execute);
            assertEquals("Teh bork", e.getMessage());
        }

        @Test
        void throwsOnCancellation() {
            queryExecutionHelper.clearGetQueryExecutionResponseQueue();
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.RUNNING);
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.CANCELLED, "Very cancel");
            SQLException e = assertThrows(SQLException.class, this::execute);
            assertEquals("Very cancel", e.getMessage());
        }

        @Test
        void executeAgainClosesPreviousResultSet() throws Exception {
            queryExecutionHelper.queueStartQueryResponse("Q2345");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
            execute();
            ResultSet rs1 = statement.getResultSet();
            execute();
            ResultSet rs2 = statement.getResultSet();
            assertTrue(rs1.isClosed());
            assertFalse(rs2.isClosed());
        }

        @Nested
        class WhenInterruptedWhileSleeping {
            @BeforeEach
            void setUp() {
                statement = new AthenaStatement(createConfiguration().withNetworkTimeout(Duration.ofMillis(10)), clock);
            }

            @Test
            void throwsWhenStartQueryExecutionDurationExceedsNetworkTimeout() {
                queryExecutionHelper.queueStartQueryResponse("Q1234");
                queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
                queryExecutionHelper.delayStartQueryExecutionResponses(Duration.ofMillis(100));
                assertThrows(SQLTimeoutException.class, () -> statement.executeQuery("SELECT 1"));
            }

            @Test
            void throwsWhenGetQueryExecutionDurationExceedsNetworkTimeout() {
                queryExecutionHelper.queueStartQueryResponse("Q1234");
                queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
                queryExecutionHelper.delayGetQueryExecutionResponses(Duration.ofMillis(100));
                assertThrows(SQLTimeoutException.class, () -> statement.executeQuery("SELECT 1"));
            }
        }
    }

    @Nested
    class Execute extends SharedExecuteTests<Boolean> {
        @Override
        protected Boolean execute() throws SQLException {
            return statement.execute("SELECT 1");
        }

        @Test
        void returnsTrue() throws Exception {
            assertTrue(execute());
        }

        @Nested
        class WhenInterruptedWhileSleeping {
            private Thread runner;
            private AtomicReference<Boolean> executeResult;
            private AtomicReference<Throwable> executeThrowable;
            private AtomicReference<Boolean> interruptedState;

            @BeforeEach
            void setUp() {
                pollingStrategy = (callback, deadline) -> {
                    throw new InterruptedException();
                };
                executeResult = new AtomicReference<>(null);
                executeThrowable = new AtomicReference<>(null);
                interruptedState = new AtomicReference<>(null);
                runner = new Thread(() -> {
                    try {
                        executeResult.set(execute());
                    } catch (Throwable t) {
                        executeThrowable.set(t);
                    } finally {
                        interruptedState.set(Thread.currentThread().isInterrupted());
                    }
                });
            }

            @Test
            void setsTheInterruptFlag() throws Exception {
                runner.start();
                runner.join();
                assertTrue(interruptedState.get());
            }

            @Test
            void throwsAnSQLException() throws Exception {
                runner.start();
                runner.join();
                assertNull(executeResult.get());
                assertTrue(SQLException.class.isAssignableFrom(executeThrowable.get().getClass()));
            }
        }
    }

    @Nested
    class ExecuteQuery extends SharedExecuteTests<ResultSet> {
        @Override
        protected ResultSet execute() throws SQLException {
            return statement.executeQuery("SELECT 1");
        }

        @Test
        void returnsResultSet() throws Exception {
            assertNotNull(execute());
        }

        @Test
        @Override
        void executeAgainClosesPreviousResultSet() throws Exception {
            queryExecutionHelper.queueStartQueryResponse("Q234");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
            ResultSet rs1 = execute();
            ResultSet rs2 = execute();
            assertTrue(rs1.isClosed());
            assertFalse(rs2.isClosed());
        }

        @Nested
        class WhenTheResultSetIsUsed {
            @Test
            void createsAResultFromTheQueryExecution() throws Exception {
                ResultSet rs = execute();
                rs.next();
                assertEquals("Q1234", resultFactoryQueryExecution.queryExecutionId());
            }

            @Test
            void proxiesToTheResultInstance1() throws Exception {
                ResultSet rs = execute();
                rs.next();
                verify(result).next();
            }

            @Test
            void proxiesToTheResultInstance2() throws Exception {
                ResultSet rs = execute();
                rs.getMetaData();
                verify(result).getMetaData();
            }
        }
    }

    @Nested
    class ExecuteWithAutoGeneratedKeys {
        @Nested
        class WhenGivenGenerateKeys {
            @Test
            void isNotSupported() {
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("INSERT INTO foo (a) VALUES (1)", Statement.RETURN_GENERATED_KEYS));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("INSERT INTO foo (a) VALUES (1)", new int[]{1}));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("INSERT INTO foo (a) VALUES (1)", new String[]{"a"}));
            }
        }

        @Nested
        class WhenGivenNoKeys extends SharedExecuteTests<Boolean> {
            @Override
            protected Boolean execute() throws SQLException {
                return statement.execute("SELECT 1", Statement.NO_GENERATED_KEYS);
            }
        }
    }

    @Nested
    class ExecuteUpdate extends SharedExecuteTests<Integer> {
        @Override
        protected Integer execute() throws SQLException {
            return statement.executeUpdate("SELECT 1");
        }

        @Test
        void alwaysReturnsZero() throws Exception {
            assertEquals(0, execute());
        }
    }

    @Nested
    class ExecuteLargeUpdate extends SharedExecuteTests<Long> {
        @Override
        protected Long execute() throws SQLException {
            return statement.executeLargeUpdate("SELECT 1");
        }

        @Test
        void alwaysReturnsZero() throws Exception {
            assertEquals(0, execute());
        }
    }

    @Nested
    class ExecuteUpdateWithAutoGeneratedKeys {
        @Nested
        class WhenGivenGenerateKeys {
            @Test
            void isNotSupported() {
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("UPDATE foo SET bar = 1", new int[0]));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("UPDATE foo SET bar = 1", new String[0]));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("UPDATE foo SET bar = 1", Statement.RETURN_GENERATED_KEYS));
            }
        }

        @Nested
        class WhenGivenNoKeys extends SharedExecuteTests<Integer> {
            @Override
            protected Integer execute() throws SQLException {
                return statement.executeUpdate("SELECT 1", Statement.NO_GENERATED_KEYS);
            }
        }
    }

    @Nested
    class ExecuteLargeUpdateWithAutoGeneratedKeys {
        @Nested
        class WhenGivenGenerateKeys {
            @Test
            void isNotSupported() {
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeLargeUpdate("UPDATE foo SET bar = 1", new int[0]));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeLargeUpdate("UPDATE foo SET bar = 1", new String[0]));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeLargeUpdate("UPDATE foo SET bar = 1", Statement.RETURN_GENERATED_KEYS));
            }
        }

        @Nested
        class WhenGivenNoKeys extends SharedExecuteTests<Long> {
            @Override
            protected Long execute() throws SQLException {
                return statement.executeLargeUpdate("SELECT 1", Statement.NO_GENERATED_KEYS);
            }
        }
    }

    @Nested
    class Close extends SharedExecuteSetup {
        @Test
        void isClosedAfterClose() throws Exception {
            statement.close();
            assertTrue(statement.isClosed());
        }

        @Test
        void closesResultSet() throws Exception {
            ResultSet rs = statement.executeQuery("SELECT 1");
            statement.close();
            assertTrue(rs.isClosed());
        }
    }

    @Nested
    class GetResultSet extends SharedExecuteSetup {
        @Test
        void returnsNullBeforeExecute() {
            assertNull(statement.getResultSet());
        }

        @Nested
        class AfterExecuteIsCalled {
            @Test
            void returnsTheSameResultSetAsExecuteQuery() throws Exception {
                ResultSet rs1 = statement.executeQuery("SELECT 1");
                ResultSet rs2 = statement.getResultSet();
                assertSame(rs1, rs2);
            }

            @Test
            void returnsAResultSet() throws Exception {
                statement.execute("SELECT 1");
                assertNotNull(statement.getResultSet());
            }
        }
    }

    @Nested
    class Unwrap {
        @Test
        void returnsTypedInstance() throws SQLException {
            AthenaStatement ac = statement.unwrap(AthenaStatement.class);
            assertNotNull(ac);
        }

        @Test
        void throwsWhenAskedToUnwrapClassItIsNotWrapperFor() {
            assertThrows(SQLException.class, () -> statement.unwrap(String.class));
        }
    }

    @Nested
    class IsWrapperFor {
        @Test
        void isWrapperForAthenaStatement() {
            assertTrue(statement.isWrapperFor(AthenaStatement.class));
        }

        @Test
        void isWrapperForObject() {
            assertTrue(statement.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() {
            assertFalse(statement.isWrapperFor(String.class));
        }
    }

    @Nested
    class GetQueryTimeout {
        @Test
        void returnsTheConfiguredTimeoutInSeconds() {
            assertEquals(60, statement.getQueryTimeout());
        }

        @Test
        void returnsTheValueSetWithSetQueryTimeout() {
            statement.setQueryTimeout(99);
            assertEquals(99, statement.getQueryTimeout());
        }
    }

    @Nested
    class SetQueryTimeout {
        @BeforeEach
        void setUp() {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.QUEUED);
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
        }

        @Test
        void setsTheTimeoutUsedForStartQueryExecution() throws SQLException {
            queryExecutionHelper.delayStartQueryExecutionResponses(Duration.ofMillis(10));
            statement.setQueryTimeout(0);
            assertThrows(SQLTimeoutException.class, () -> statement.executeQuery("SELECT 1"));
        }

        @Test
        void setsTheTimeoutUsedForGetQueryExecution() throws SQLException {
            queryExecutionHelper.delayGetQueryExecutionResponses(Duration.ofMillis(10));
            statement.setQueryTimeout(0);
            assertThrows(SQLTimeoutException.class, () -> statement.executeQuery("SELECT 1"));
        }

        @Test
        void setsTheTimeoutUsedForQuerySpanningMultipleOperations() {
            queryExecutionHelper.delayStartQueryExecutionResponses(Duration.ofMillis(40));
            queryExecutionHelper.delayGetQueryExecutionResponses(Duration.ofMillis(40));
            statement.setQueryTimeout(Duration.ofMillis(100));
            assertThrows(SQLTimeoutException.class, () -> statement.executeQuery("SELECT 1"));
        }

        @Test
        void cancelsQueryAfterTimeout() throws Exception {
            queryExecutionHelper.delayGetQueryExecutionResponses(Duration.ofMillis(10));
            statement.setQueryTimeout(0);
            try { statement.executeQuery("SELECT 1"); } catch (SQLTimeoutException ste) { /* expected */ }
            StopQueryExecutionRequest request = queryExecutionHelper.stopQueryExecutionRequests().get(0);
            assertEquals("Q1234", request.queryExecutionId());
        }

        @Test
        void doesNotCancelQueryThatDidNotStart() throws Exception {
            queryExecutionHelper.delayStartQueryExecutionResponses(Duration.ofMillis(10));
            statement.setQueryTimeout(0);
            try { statement.executeQuery("SELECT 1"); } catch (SQLTimeoutException ste) { /* expected */ }
            assertEquals(0, queryExecutionHelper.stopQueryExecutionRequests().size());
        }
    }

    @Nested
    class Cancel {
        @Nested
        class WhenCalledBeforeExecute {
            @Test
            void throwsAnException() {
                assertThrows(SQLException.class, () -> statement.cancel());
            }
        }

        @Nested
        class WhenCalledAfterExecute {
            @Test
            void sendsACancelRequest() throws Exception {
                queryExecutionHelper.blockGetQueryExecutionResponse();
                queryExecutionHelper.queueStartQueryResponse("Q2345");
                queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.RUNNING);
                queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
                Thread runner = new Thread(() -> {
                    try {
                        statement.execute("SELECT 1");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                runner.start();
                while (queryExecutionHelper.getQueryExecutionRequests().size() == 0) {
                    Thread.sleep(1);
                }
                statement.cancel();
                queryExecutionHelper.unblockGetQueryExecutionResponse();
                runner.join();
                StopQueryExecutionRequest request = queryExecutionHelper.stopQueryExecutionRequests().get(0);
                assertEquals("Q2345", request.queryExecutionId());
            }
        }

        @Nested
        class WhenCalledAfterQueryCompletion {
            @Test
            void throwsAnException() throws Exception {
                queryExecutionHelper.queueStartQueryResponse("Q1234");
                queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
                statement.execute("SELECT 1");
                assertThrows(SQLException.class, () -> statement.cancel());
            }
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnException() throws Exception {
                statement.close();
                assertThrows(SQLException.class, () -> statement.cancel());
            }
        }
    }

    @Nested
    class SetClientRequestTokenProvider extends SharedExecuteSetup {
        @Test
        void passesTheExecutedSqlToTheProvider() throws Exception {
            AtomicReference<String> passedSql = new AtomicReference<>(null);
            statement.setClientRequestTokenProvider(sql -> {
                passedSql.set(sql);
                return Optional.of("foo");
            });
            statement.execute("SELECT 1");
            assertEquals("SELECT 1", passedSql.get());
        }

        @Test
        void usesTheReturnValueAsClientRequestToken() throws Exception {
            statement.setClientRequestTokenProvider(sql -> Optional.of("foo"));
            statement.execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("foo", request.clientRequestToken());
        }

        @Nested
        class WhenGivenNull {
            @Test
            void usesNullAsTheClientRequestToken() throws Exception {
                statement.setClientRequestTokenProvider(sql -> Optional.of("foo"));
                statement.setClientRequestTokenProvider(null);
                statement.execute("SELECT 1");
                StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
                assertNull(request.clientRequestToken());
            }
        }
    }

    @Nested
    class GetFetchDirection {
        @Test
        void returnsForwardOnly() {
            assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
        }
    }

    @Nested
    class SetFetchDirection {
        @Nested
        class WhenGivenForward {
            @Test
            void doesNothing() throws Exception {
                statement.setFetchDirection(ResultSet.FETCH_FORWARD);
                assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
            }
        }

        @Nested
        class WhenGivenAnyOtherSetting {
            @Test
            void throwsException() {
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setFetchDirection(ResultSet.FETCH_UNKNOWN));
                assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setFetchDirection(9999));
            }
        }
    }
}
