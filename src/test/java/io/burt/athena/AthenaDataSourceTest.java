package io.burt.athena;

import io.burt.athena.support.QueryExecutionHelper;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(TestNameGenerator.class)
class AthenaDataSourceTest {
    private ConnectionConfigurationFactory connectionConfigurationFactory;
    private AthenaDataSource dataSource;
    private QueryExecutionHelper queryExecutionHelper;

    @BeforeEach
    void setUp() {
        connectionConfigurationFactory = spy(new ConnectionConfigurationFactory());
        lenient().when(connectionConfigurationFactory.createConnectionConfiguration(any(), any(), any(), any(), any(), any())).then(invocation -> {
            ConnectionConfiguration cc = (ConnectionConfiguration) invocation.callRealMethod();
            cc = spy(cc);
            lenient().when(cc.athenaClient()).thenReturn(queryExecutionHelper);
            return cc;
        });
        dataSource = new AthenaDataSource(connectionConfigurationFactory);
        queryExecutionHelper = new QueryExecutionHelper();
    }

    @Nested
    class GetConnection {
        @Test
        void returnsAConnection() throws Exception {
            dataSource.setRegion("sa-east-1");
            assertNotNull(dataSource.getConnection());
        }

        @Test
        void returnsAnAthenaConnection() throws Exception {
            dataSource.setRegion("sa-east-1");
            assertDoesNotThrow(() -> dataSource.getConnection().unwrap(AthenaConnection.class));
            assertNotNull(dataSource.getConnection().unwrap(AthenaConnection.class));
        }

        @Test
        void createsAnAthenaClientForTheConfiguredRegion() throws Exception {
            dataSource.setRegion("sa-east-1");
            dataSource.getConnection();
            verify(connectionConfigurationFactory).createConnectionConfiguration(eq(Region.SA_EAST_1), any(), any(), any(), any(), any());
        }

        @Test
        void usesTheDefaultDatabaseByDefault() throws Exception {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
            dataSource.setRegion("sa-east-1");
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("default", request.queryExecutionContext().database());
        }

        @Nested
        class WhenGivenUsernameAndPassword {
            @Test
            void throwsAnException() {
                Exception e = assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.getConnection("foo", "bar"));
                assertTrue(e.getMessage().contains("IAM credentials"));
            }
        }
    }

    @Nested
    class SetRegion {
        @Nested
        class WhenGivenAString {
            @Test
            void setsTheRegionOfTheAthenaClient() throws Exception {
                dataSource.setRegion("ca-central-1");
                dataSource.getConnection();
                verify(connectionConfigurationFactory).createConnectionConfiguration(eq(Region.CA_CENTRAL_1), any(), any(), any(), any(), any());
            }
        }
    }

    @Nested
    class SetDatabase {
        @Test
        void usesTheDatabaseWhenQuerying() throws Exception {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
            dataSource.setRegion("sa-east-1");
            dataSource.setDatabase("test_db");
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("test_db", request.queryExecutionContext().database());
        }
    }

    @Nested
    class SetWorkGroup {
        @Test
        void usesTheWorkGroupWhenQuerying() throws Exception {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
            dataSource.setRegion("sa-east-1");
            dataSource.setWorkGroup("test_wg");
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("test_wg", request.workGroup());
        }
    }

    @Nested
    class SetOutputLocation {
        @Test
        void usesTheWorkGroupWhenQuerying() throws Exception {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
            dataSource.setRegion("sa-east-1");
            dataSource.setOutputLocation("s3://test/location");
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("s3://test/location", request.resultConfiguration().outputLocation());
        }
    }

    @Nested
    class IsWrapperFor {
        @Test
        void isWrapperForAthenaDataSource() throws Exception {
            assertTrue(dataSource.isWrapperFor(AthenaDataSource.class));
        }

        @Test
        void isWrapperForObject() throws Exception {
            assertTrue(dataSource.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() throws Exception {
            assertFalse(dataSource.isWrapperFor(String.class));
        }
    }

    @Nested
    class Unwrap {
        @Test
        void returnsTypedInstance() throws SQLException {
            AthenaDataSource ac = dataSource.unwrap(AthenaDataSource.class);
            assertNotNull(ac);
        }

        @Test
        void throwsWhenAskedToUnwrapClassItIsNotWrapperFor() {
            assertThrows(SQLException.class, () -> dataSource.unwrap(String.class));
        }
    }

    @Nested
    class GetLogWriter {
        @Test
        void alwaysReturnsNull() throws Exception {
            assertNull(dataSource.getLogWriter());
        }
    }

    @Nested
    class SetLogWriter {
        @Test
        void doesNothing() throws Exception {
            dataSource.setLogWriter(new PrintWriter(System.err));
            assertNull(dataSource.getLogWriter());
        }
    }

    @Nested
    class GetLoginTimeout {
        @Test
        void alwaysReturnsZero() throws Exception {
            assertEquals(0, dataSource.getLoginTimeout());
        }
    }

    @Nested
    class SetLoginTimeout {
        @Test
        void doesNothing() throws Exception {
            dataSource.setLoginTimeout(99);
            assertEquals(0, dataSource.getLoginTimeout());
        }
    }

    @Nested
    class GetParentLogger {
        @Test
        void throwsAnException() {
            Exception e = assertThrows(SQLFeatureNotSupportedException.class, () -> dataSource.getParentLogger());
            assertTrue(e.getMessage().contains("java.util.logging is not used by this data source"));
        }
    }
}
