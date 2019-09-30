package io.burt.athena;

import io.burt.athena.configuration.ConnectionConfiguration;
import io.burt.athena.polling.PollingStrategy;
import io.burt.athena.support.ConfigurableConnectionConfiguration;
import io.burt.athena.support.QueryExecutionHelper;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class AthenaConnectionTest {
    private PollingStrategy pollingStrategy;
    private QueryExecutionHelper queryExecutionHelper;
    private ConnectionConfiguration connectionConfiguration;
    private AthenaConnection connection;

    @BeforeEach
    void setUpConnection() {
        pollingStrategy = createPollingStrategy();
        queryExecutionHelper = new QueryExecutionHelper();
        connectionConfiguration = spy(createConfiguration());
        connection = new AthenaConnection(connectionConfiguration);
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

    private ConnectionConfiguration createConfiguration() {
        return new ConfigurableConnectionConfiguration(
                "test_db",
                "test_wg",
                "s3://test/location",
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                () -> queryExecutionHelper,
                () -> null,
                () -> pollingStrategy,
                (q) -> null
        );
    }

    class SharedQuerySetup {
        StartQueryExecutionRequest execute() throws Exception {
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1");
            List<StartQueryExecutionRequest> requests = queryExecutionHelper.startQueryRequests();
            return requests.get(requests.size() - 1);
        }

        @BeforeEach
        void setUp() {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
            queryExecutionHelper.queueGetQueryExecutionResponse(b -> b.queryExecution(bb -> bb.status(bbb -> bbb.state(QueryExecutionState.SUCCEEDED))));
        }
    }

    @Nested
    class CreateStatement {
        @Test
        void returnsStatement() throws Exception {
            assertNotNull(connection.createStatement());
        }

        @Nested
        class WhenTheStatementIsExecuted extends SharedQuerySetup {
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
        class WhenGivenAnUnsupportedResultSetType {
            @Test
            void throwsAnError() {
                assertThrows(SQLFeatureNotSupportedException.class, () -> connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
            }
        }

        @Nested
        class WhenGivenAnUnsupportedResultSetConcurrency {
            @Test
            void throwsAnError() {
                assertThrows(SQLFeatureNotSupportedException.class, () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
            }
        }

        @Nested
        class WhenGivenAHoldability {
            @Test
            void throwsAnError() {
                assertThrows(SQLFeatureNotSupportedException.class, () -> connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));
            }
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.createStatement());
            }
        }
    }

    @Nested
    class PrepareStatement {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement("SELECT ?"));
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement("SELECT ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement("SELECT ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement("SELECT ?", new int[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement("SELECT ?", new String[0]));
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareStatement("SELECT ?", Statement.NO_GENERATED_KEYS));
        }
    }

    @Nested
    class PrepareCall {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall("CALL something"));
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall("CALL something", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.prepareCall("CALL something", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT));
        }
    }

    @Nested
    class NativeSql {
        @Test
        void returnsTheSql() throws Exception {
            assertEquals("SELECT 1", connection.nativeSQL("SELECT 1"));
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.nativeSQL("SELECT 1"));
            }
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
        @Test
        void closesTheConfiguration() throws Exception {
            connection.close();
            verify(connectionConfiguration).close();
        }


    }

    @Nested
    class IsClosed {
        @Test
        void returnsFalseWhenOpen() {
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
        void returnsTrueWhenOpen() {
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
        void isWrapperForAthenaConnection() {
            assertTrue(connection.isWrapperFor(AthenaConnection.class));
        }

        @Test
        void isWrapperForObject() {
            assertTrue(connection.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() {
            assertFalse(connection.isWrapperFor(String.class));
        }
    }

    @Nested
    class IsReadOnly {
        @Test
        void alwaysReturnsTrue() throws Exception {
            assertTrue(connection.isReadOnly());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.isReadOnly());
            }
        }
    }

    @Nested
    class SetReadOnly {
        @Test
        void ignoresTheCall() throws Exception {
            connection.setReadOnly(false);
            assertTrue(connection.isReadOnly());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.setReadOnly(true));
            }
        }
    }

    @Nested
    class SetSchema extends SharedQuerySetup {
        @Test
        void setsTheDefaultDatabaseForQueries() throws Exception {
            connection.setSchema("some_database");
            StartQueryExecutionRequest request = execute();
            assertEquals("some_database", request.queryExecutionContext().database());
        }
    }

    @Nested
    class GetSchema {
        @Test
        void returnsConfiguredDatabaseByDefault() {
            assertEquals("test_db", connection.getSchema());
        }

        @Test
        void returnsTheValuePassedInSetSchema() {
            connection.setSchema("some_database");
            assertEquals("some_database", connection.getSchema());
        }
    }

    @Nested
    class SetCatalog {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setCatalog("foo"));
        }
    }

    @Nested
    class GetCatalog {
        @Test
        void returnsAStaticValue() {
            assertEquals("AwsDataCatalog", connection.getCatalog());
        }
    }

    @Nested
    class GetAutoCommit {
        @Test
        void alwaysReturnsTrue() throws Exception {
            assertTrue(connection.getAutoCommit());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.getAutoCommit());
            }
        }
    }

    @Nested
    class SetAutoCommit {
        @Test
        void ignoresTheCall() throws Exception {
            connection.setAutoCommit(false);
            assertTrue(connection.getAutoCommit());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.setAutoCommit(false));
            }
        }
    }

    @Nested
    class GetTransactionIsolation {
        @Test
        void alwaysReturnsNone() throws Exception {
            assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.getTransactionIsolation());
            }
        }
    }

    @Nested
    class SetTransactionIsolation {
        @Test
        void ignoresTheCall() throws Exception {
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.setTransactionIsolation(Connection.TRANSACTION_NONE));
            }
        }
    }

    @Nested
    class ClearWarnings {
        @Test
        void ignoresTheCall() throws Exception {
            connection.clearWarnings();
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.clearWarnings());
            }
        }
    }

    @Nested
    class GetWarnings {
        @Test
        void returnsNull() throws Exception {
            assertNull(connection.getWarnings());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.getWarnings());
            }
        }
    }

    @Nested
    class Commit {
        @Test
        void ignoresTheCalls() throws Exception {
            connection.commit();
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.commit());
            }
        }
    }

    @Nested
    class Rollback {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.rollback());
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.rollback(mock(Savepoint.class)));
        }
    }

    @Nested
    class GetHoldability {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.getHoldability());
        }
    }

    @Nested
    class SetHoldability {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        }
    }

    @Nested
    class SetSavepoint {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setSavepoint());
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setSavepoint("foo"));
        }
    }

    @Nested
    class ReleaseSavepoint {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.releaseSavepoint(mock(Savepoint.class)));
        }
    }

    @Nested
    class SetClientInfo {
        @Test
        void ignoresTheCalls() throws Exception {
            connection.setClientInfo(new Properties());
            connection.setClientInfo("name", "value");
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.setClientInfo(new Properties()));
            }
        }
    }

    @Nested
    class GetClientInfo {
        @Test
        void alwaysReturnsAnEmptyPropertiesObject() throws Exception {
            assertEquals(new Properties(), connection.getClientInfo());
            assertNull(connection.getClientInfo("name"));
            connection.setClientInfo("name", "value");
            assertEquals(new Properties(), connection.getClientInfo());
            assertNull(connection.getClientInfo("name"));
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.getClientInfo());
            }
        }
    }

    @Nested
    class GetTypeMap {
        @Test
        void returnsAnEmptyMap() throws Exception {
            assertTrue(connection.getTypeMap().isEmpty());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.getTypeMap());
            }
        }
    }

    @Nested
    class SetTypeMap {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setTypeMap(Collections.emptyMap()));
        }
    }

    @Nested
    class SetNetworkTimeout extends SharedQuerySetup {
        @Test
        void setsTheTimeoutUsedForApiCalls1() throws Exception {
            queryExecutionHelper.delayStartQueryExecutionResponses(Duration.ofMillis(10));
            connection.setNetworkTimeout(ForkJoinPool.commonPool(), 0);
            assertThrows(SQLTimeoutException.class, this::execute);
        }

        @Test
        void setsTheTimeoutUsedForApiCalls2() throws Exception {
            queryExecutionHelper.delayGetQueryExecutionResponses(Duration.ofMillis(10));
            connection.setNetworkTimeout(ForkJoinPool.commonPool(), 0);
            assertThrows(SQLTimeoutException.class, this::execute);
        }

        @Test
        void ignoresTheExecutorArgument() {
            assertDoesNotThrow(() -> connection.setNetworkTimeout(null, 10));
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.setNetworkTimeout(null, 10));
            }
        }
    }

    @Nested
    class GetNetworkTimeout {
        @Test
        void returnsTheConfiguredTimeout() throws Exception {
            assertTrue(connection.getNetworkTimeout() > -1);
        }

        @Test
        void returnsTheValueSetWithSetNetworkTimeout() throws Exception {
            connection.setNetworkTimeout(ForkJoinPool.commonPool(), 999);
            assertEquals(999, connection.getNetworkTimeout());
        }

        @Nested
        class WhenClosed {
            @Test
            void throwsAnError() throws Exception {
                connection.close();
                assertThrows(SQLException.class, () -> connection.getNetworkTimeout());
            }
        }
    }

    @Nested
    class GetMetaData {
        @Test
        void returnsADatabaseMetaDataObject() {
            assertNotNull(connection.getMetaData());
        }

        @Test
        void returnsTheSameMetaDataEveryTime() {
            DatabaseMetaData md1 = connection.getMetaData();
            DatabaseMetaData md2 = connection.getMetaData();
            assertSame(md1, md2);
        }
    }
}
