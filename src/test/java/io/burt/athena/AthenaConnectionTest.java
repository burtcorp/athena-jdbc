package io.burt.athena;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AthenaConnectionTest {
    private AthenaClient athenaClient;
    private AthenaConnection connection;

    @BeforeEach
    void setUpConnection() {
        ConnectionConfiguration configuration = new ConnectionConfiguration("test_db", "test_wg", "s3://test/location");
        athenaClient = mock(AthenaClient.class);
        connection = new AthenaConnection(athenaClient, configuration);
        StartQueryExecutionResponse startQueryResponse = StartQueryExecutionResponse.builder().queryExecutionId("Q1234").build();
        when(athenaClient.startQueryExecution(ArgumentMatchers.<Consumer<StartQueryExecutionRequest.Builder>>any())).thenReturn(startQueryResponse);
        QueryExecutionStatus status = QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build();
        QueryExecution queryExecution = QueryExecution.builder().status(status).build();
        GetQueryExecutionResponse getQueryResponse = GetQueryExecutionResponse.builder().queryExecution(queryExecution).build();
        when(athenaClient.getQueryExecution(ArgumentMatchers.<Consumer<GetQueryExecutionRequest.Builder>>any())).thenReturn(getQueryResponse);
    }

    @Nested
    class CreateStatement {
        @Captor
        ArgumentCaptor<Consumer<StartQueryExecutionRequest.Builder>> startQueryExecutionCaptor;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.initMocks(this);
        }

        private StartQueryExecutionRequest execute() throws Exception {
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1");
            verify(athenaClient).startQueryExecution(startQueryExecutionCaptor.capture());
            StartQueryExecutionRequest.Builder builder = StartQueryExecutionRequest.builder();
            startQueryExecutionCaptor.getValue().accept(builder);
            return builder.build();
        }

        @Test
        void returnsStatement() throws Exception {
            assertNotNull(connection.createStatement());
        }

        @Test
        void statementStartsQuery() throws Exception {
            StartQueryExecutionRequest request = execute();
            assertEquals("SELECT 1", request.queryString());
        }

        @Test
        void queryExecutesInTheConfiguredDatabase() throws Exception {
            StartQueryExecutionRequest request = execute();
            assertEquals("test_db", request.queryExecutionContext().database());
        }

        @Test
        void queryExecutesInTheConfiguredWorkgroup() throws Exception {
            StartQueryExecutionRequest request = execute();
            assertEquals("test_wg", request.workGroup());
        }

        @Test
        void queryExecutesWithTheConfiguredOutputLocation() throws Exception {
            StartQueryExecutionRequest request = execute();
            assertEquals("s3://test/location", request.resultConfiguration().outputLocation());
        }
    }

    @Nested
    class PrepareStatement {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> {
                connection.prepareStatement("SELECT ?");
            });
        }
    }

    @Nested
    class PrepareCall {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> {
                connection.prepareCall("CALL something");
            });
        }
    }

    @Nested
    class NativeSql {
        @Test
        void returnsTheSql() throws Exception {
            assertEquals("SELECT 1", connection.nativeSQL("SELECT 1"));
        }
    }

    @Nested
    class Unwrap {
        @Test
        void returnsTypedInstance() throws SQLException {
            AthenaConnection ac = connection.unwrap(AthenaConnection.class);
            assertNotNull(ac);
        }

        @Test
        void throwsWhenAskedToUnwrapClassItIsNotWrapperFor() {
            assertThrows(SQLException.class, () -> connection.unwrap(String.class));
        }
    }

    @Nested
    class Close {
        // TODO
    }

    @Nested
    class IsClosed {
        @Test
        void returnsFalseWhenOpen() throws Exception {
            assertFalse(connection.isClosed());
        }

        @Test
        void returnsTrueWhenClosed() throws Exception {
            connection.close();
            assertTrue(connection.isClosed());
        }
    }

    @Nested
    class IsValid {
        @Test
        void returnsTrueWhenOpen() throws Exception {
            assertTrue(connection.isValid(0));
        }

        @Test
        void returnsFalseWhenClosed() throws Exception {
            connection.close();
            assertFalse(connection.isValid(0));
        }
    }

    @Nested
    class IsWrapperFor {
        @Test
        void isWrapperForAthenaConnection() throws Exception {
            assertTrue(connection.isWrapperFor(AthenaConnection.class));
        }

        @Test
        void isWrapperForObject() throws Exception {
            assertTrue(connection.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() throws Exception {
            assertFalse(connection.isWrapperFor(String.class));
        }
    }

    @Nested
    class IsReadOnly {
        @Test
        void alwaysReturnsTrue() throws Exception {
            assertTrue(connection.isReadOnly());
        }
    }

    @Nested
    class SetReadOnly {
        @Test
        void ignoresTheCall() throws Exception {
            connection.setReadOnly(false);
            assertTrue(connection.isReadOnly());
        }
    }

    @Nested
    class GetAutoCommit {
        @Test
        void alwaysReturnsTrue() throws Exception {
            assertTrue(connection.getAutoCommit());
        }
    }

    @Nested
    class SetAutoCommit {
        @Test
        void ignoresTheCall() throws Exception {
            connection.setAutoCommit(false);
            assertTrue(connection.getAutoCommit());
        }
    }

    @Nested
    class GetTransactionIsolation {
        @Test
        void alwaysReturnsNone() throws Exception {
            assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
        }
    }

    @Nested
    class SetTransactionIsolation {
        @Test
        void ignoresTheCall() throws Exception {
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
        }
    }

    @Nested
    class ClearWarnings {
        @Test
        void ignoresTheCall() throws Exception {
            connection.clearWarnings();
        }
    }
}
