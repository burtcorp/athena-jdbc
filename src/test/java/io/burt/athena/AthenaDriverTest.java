package io.burt.athena;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AthenaDriverTest {
    private AthenaDriver driver;
    private AthenaClient athenaClient;
    private AwsClientFactory clientFactory;
    private Properties defaultProperties;

    private final String jdbcUrl = "jdbc:awsathena://test_db";

    @BeforeEach
    void setUpProperties() {
        defaultProperties = new Properties();
        defaultProperties.setProperty("AWS_REGION", Region.AP_SOUTHEAST_1.toString());
        defaultProperties.setProperty("WORK_GROUP", "test_wg");
        defaultProperties.setProperty("OUTPUT_LOCATION", "s3://test/location");
    }

    @BeforeEach
    void setUpDriver() {
        clientFactory = mock(AwsClientFactory.class);
        athenaClient = mock(AthenaClient.class);
        when(clientFactory.createAthenaClient(Region.AP_SOUTHEAST_1)).thenReturn(athenaClient);
        driver = new AthenaDriver(clientFactory);
    }

    @Nested
    class Connect {
        @Captor
        ArgumentCaptor<Consumer<StartQueryExecutionRequest.Builder>> startQueryExecutionCaptor;

        @BeforeEach
        void setUp() {
            MockitoAnnotations.initMocks(this);
        }

        StartQueryExecutionRequest executeRequest() throws Exception {
            StartQueryExecutionResponse startQueryResponse = StartQueryExecutionResponse.builder().queryExecutionId("Q1234").build();
            when(athenaClient.startQueryExecution(ArgumentMatchers.<Consumer<StartQueryExecutionRequest.Builder>>any())).thenReturn(startQueryResponse);
            QueryExecutionStatus status = QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build();
            QueryExecution queryExecution = QueryExecution.builder().status(status).build();
            GetQueryExecutionResponse getQueryResponse = GetQueryExecutionResponse.builder().queryExecution(queryExecution).build();
            when(athenaClient.getQueryExecution(ArgumentMatchers.<Consumer<GetQueryExecutionRequest.Builder>>any())).thenReturn(getQueryResponse);
            Connection connection = driver.connect(jdbcUrl, defaultProperties);
            connection.createStatement().execute("SELECT 1");
            verify(athenaClient).startQueryExecution(startQueryExecutionCaptor.capture());
            StartQueryExecutionRequest.Builder builder = StartQueryExecutionRequest.builder();
            startQueryExecutionCaptor.getValue().accept(builder);
            return builder.build();
        }

        @Test
        void returnsConnection() throws Exception {
            assertNotNull(driver.connect(jdbcUrl, defaultProperties));
        }

        @Test
        void parsesTheDatabaseFromTheUrl() throws Exception {
            StartQueryExecutionRequest request = executeRequest();
            assertEquals("test_db", request.queryExecutionContext().database());
        }

        @Test
        void usesTheAwsRegionFromTheProperties() throws Exception {
            driver.connect(jdbcUrl, defaultProperties);
            verify(clientFactory).createAthenaClient(Region.AP_SOUTHEAST_1);
        }

        @Test
        void usesTheWorkGroupFromTheProperties() throws Exception {
            StartQueryExecutionRequest request = executeRequest();
            assertEquals("test_wg", request.workGroup());
        }

        @Test
        void usesTheOutputLocationFromTheProperties() throws Exception {
            StartQueryExecutionRequest request = executeRequest();
            assertEquals("s3://test/location", request.resultConfiguration().outputLocation());
        }
    }

    @Nested
    class AcceptsUrl {
        @Test
        void acceptsSimpleUrl() throws Exception {
            assertTrue(driver.acceptsURL(jdbcUrl));
        }

        @Test
        void doesNotAcceptUrlWithoutDatabase() throws Exception {
            assertFalse(driver.acceptsURL("jdbc:awsathena://"));
        }

        @Test
        void doesNotAcceptNonAthenaUrl() throws Exception {
            assertFalse(driver.acceptsURL("jdbc:postgres://localhost/db"));
        }
    }

    @Nested
    class PropertyInfo {
        @Test
        void returnsEmptyPropertyInfo() throws Exception {
            assertEquals(0, driver.getPropertyInfo(jdbcUrl, new Properties()).length);
        }
    }

    @Nested
    class Version {
        @Test
        void hasVersion() {
            assertTrue(driver.getMajorVersion() > -1);
            assertTrue(driver.getMinorVersion() > -1);
        }
    }

    @Nested
    class JdbcCompliant {
        @Test
        void isNotTrulyJdbcCompliant() {
            assertFalse(driver.jdbcCompliant());
        }
    }

    @Nested
    class ParentLogger {
        @Test
        void isNotSupported() {
            assertThrows(SQLFeatureNotSupportedException.class, () -> {
                driver.getParentLogger();
            });
        }
    }
}
