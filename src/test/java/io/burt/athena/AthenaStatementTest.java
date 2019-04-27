package io.burt.athena;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.GetQueryResultsResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AthenaStatementTest {
    @Mock private AthenaAsyncClient athenaClient;
    @Mock private PollingStrategy pollingStrategy;

    private AthenaStatement statement;

    @BeforeEach
    void setUpStatement() {
        ConnectionConfiguration configuration = new ConnectionConfiguration("test_db", "test_wg", "s3://test/location", Duration.ofMinutes(1));
        statement = new AthenaStatement(athenaClient, configuration, () -> pollingStrategy);
    }

    class SharedExecuteSetup {
        @Captor protected ArgumentCaptor<Consumer<StartQueryExecutionRequest.Builder>> startQueryExecutionCaptor;
        @Captor protected ArgumentCaptor<Consumer<GetQueryExecutionRequest.Builder>> getQueryExecutionCaptor;

        QueryExecutionState terminalState = QueryExecutionState.SUCCEEDED;
        String stateChangeReason = null;

        @BeforeEach
        void setUpStartQueryExecution() {
            StartQueryExecutionResponse response = StartQueryExecutionResponse.builder().queryExecutionId("Q1234").build();
            lenient().when(athenaClient.startQueryExecution(ArgumentMatchers.<Consumer<StartQueryExecutionRequest.Builder>>any())).thenReturn(CompletableFuture.completedFuture(response));
        }

        @BeforeEach
        void setUpGetQueryExecution() {
            final AtomicInteger callCounter = new AtomicInteger(0);
            lenient().when(athenaClient.getQueryExecution(ArgumentMatchers.<Consumer<GetQueryExecutionRequest.Builder>>any())).then(invocation -> {
                int count = callCounter.incrementAndGet();
                Consumer<GetQueryExecutionRequest.Builder> requestBuilderConsumer = invocation.getArgument(0);
                GetQueryExecutionRequest.Builder requestBuilder = GetQueryExecutionRequest.builder();
                requestBuilderConsumer.accept(requestBuilder);
                requestBuilder.build();
                QueryExecutionState state;
                if (count >= 3) {
                    state = terminalState;
                } else if (count == 1) {
                    state = QueryExecutionState.QUEUED;
                } else {
                    state = QueryExecutionState.RUNNING;
                }
                QueryExecutionStatus status = QueryExecutionStatus.builder().state(state).stateChangeReason(stateChangeReason).build();
                QueryExecution queryExecution = QueryExecution.builder().status(status).build();
                return CompletableFuture.completedFuture(GetQueryExecutionResponse.builder().queryExecution(queryExecution).build());
            });
        }
    }

    abstract class SharedExecuteTests<T> extends SharedExecuteSetup {
        protected abstract T execute() throws SQLException;

        protected StartQueryExecutionRequest executionRequest() {
            verify(athenaClient).startQueryExecution(startQueryExecutionCaptor.capture());
            StartQueryExecutionRequest.Builder builder = StartQueryExecutionRequest.builder();
            startQueryExecutionCaptor.getValue().accept(builder);
            return builder.build();
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
            verify(athenaClient, atLeastOnce()).getQueryExecution(getQueryExecutionCaptor.capture());
            GetQueryExecutionRequest.Builder builder = GetQueryExecutionRequest.builder();
            getQueryExecutionCaptor.getValue().accept(builder);
            assertEquals("Q1234", builder.build().queryExecutionId());
        }

        @Test
        void pollsUntilSucceeded() throws Exception {
            execute();
            verify(athenaClient, times(3)).getQueryExecution(ArgumentMatchers.<Consumer<GetQueryExecutionRequest.Builder>>any());
        }

        @Test
        void sleepsBetweenPolls() throws Exception {
            execute();
            verify(pollingStrategy, times(2)).waitUntilNext();
        }

        @Test
        void throwsOnFailure() {
            terminalState = QueryExecutionState.FAILED;
            stateChangeReason = "Teh bork";
            SQLException e = assertThrows(SQLException.class, this::execute);
            assertEquals("Teh bork", e.getMessage());
        }

        @Test
        void throwsOnCancellation() {
            terminalState = QueryExecutionState.CANCELLED;
            stateChangeReason = "Very cancel";
            SQLException e = assertThrows(SQLException.class, this::execute);
            assertEquals("Very cancel", e.getMessage());
        }

        @Test
        void executeAgainClosesPreviousResultSet() throws Exception {
            execute();
            ResultSet rs1 = statement.getResultSet();
            execute();
            ResultSet rs2 = statement.getResultSet();
            assertTrue(rs1.isClosed());
            assertFalse(rs2.isClosed());
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
            private AtomicReference<Boolean> interruptedState;

            @BeforeEach
            void setUp() throws Exception {
                executeResult = new AtomicReference<>(null);
                interruptedState = new AtomicReference<>(null);
                runner = new Thread(() -> {
                    try {
                        executeResult.set(execute());
                        interruptedState.set(Thread.currentThread().isInterrupted());
                    } catch (SQLException sqle) {
                        throw new RuntimeException(sqle);
                    }
                });
                doThrow(InterruptedException.class).when(pollingStrategy).waitUntilNext();
            }

            @Test
            void setsTheInterruptFlag() throws Exception {
                runner.start();
                runner.join();
                assertTrue(interruptedState.get());
            }

            @Test
            void returnsFalse() throws Exception {
                runner.start();
                runner.join();
                assertFalse(executeResult.get());
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
            ResultSet rs1 = execute();
            ResultSet rs2 = execute();
            assertTrue(rs1.isClosed());
            assertFalse(rs2.isClosed());
        }

        @Nested
        class WhenTheResultSetIsUsed {
            @Captor private ArgumentCaptor<Consumer<GetQueryResultsRequest.Builder>> getQueryResultsCaptor;

            @BeforeEach
            void setUpGetQueryResults() {
                GetQueryResultsResponse response = GetQueryResultsResponse.builder().resultSet(rsb -> {
                    rsb.resultSetMetadata(rsmb -> rsmb.columnInfo(new ArrayList<>()));
                    rsb.rows(new ArrayList<>());
                }).build();
                when(athenaClient.getQueryResults(ArgumentMatchers.<Consumer<GetQueryResultsRequest.Builder>>any())).thenReturn(CompletableFuture.completedFuture(response));
            }

            @Test
            void queriesForResultMetadata() throws Exception {
                ResultSet rs = execute();
                rs.next();
                verify(athenaClient).getQueryResults(getQueryResultsCaptor.capture());
                GetQueryResultsRequest.Builder builder = GetQueryResultsRequest.builder();
                getQueryResultsCaptor.getValue().accept(builder);
                assertEquals("Q1234", builder.build().queryExecutionId());
            }
        }
    }

    @Nested
    class ExecuteWithAutoGeneratedKeys {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("INSERT INTO foo (a) VALUES (1)", 1));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("INSERT INTO foo (a) VALUES (1)", new int[]{1}));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute("INSERT INTO foo (a) VALUES (1)", new String[]{"a"}));
        }
    }

    @Nested
    class ExecuteUpdate {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("UPDATE foo SET bar = 1"));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("UPDATE foo SET bar = 1", new int[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("UPDATE foo SET bar = 1", new String[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate("UPDATE foo SET bar = 1", 0));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeLargeUpdate("UPDATE foo SET bar = 1"));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeLargeUpdate("UPDATE foo SET bar = 1", new int[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeLargeUpdate("UPDATE foo SET bar = 1", new String[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeLargeUpdate("UPDATE foo SET bar = 1", 0));
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
        void returnsNullBeforeExecute() throws Exception {
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
        void isWrapperForAthenaStatement() throws Exception {
            assertTrue(statement.isWrapperFor(AthenaStatement.class));
        }

        @Test
        void isWrapperForObject() throws Exception {
            assertTrue(statement.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() throws Exception {
            assertFalse(statement.isWrapperFor(String.class));
        }
    }
}
