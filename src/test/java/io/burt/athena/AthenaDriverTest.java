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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.QueryExecutionStatus;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;

import java.sql.Connection;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AthenaDriverTest {
    @Mock private AwsClientFactory clientFactory;
    @Mock private AthenaAsyncClient athenaClient;

    private AthenaDriver driver;
    private Properties defaultProperties;
    private Map<String, String> env;

    private final String jdbcUrl = "jdbc:awsathena://test_db";

    @BeforeEach
    void setUpDriver() {
        env = new HashMap<>();
        driver = new AthenaDriver(clientFactory, env);
    }

    @BeforeEach
    void setUpProperties() {
        defaultProperties = new Properties();
        defaultProperties.setProperty("AWS_REGION", Region.AP_SOUTHEAST_1.toString());
        defaultProperties.setProperty("WORK_GROUP", "test_wg");
        defaultProperties.setProperty("OUTPUT_LOCATION", "s3://test/location");
    }

    @Nested
    class Connect {
        @Captor ArgumentCaptor<Consumer<StartQueryExecutionRequest.Builder>> startQueryExecutionCaptor;

        @BeforeEach
        void setUpDriver() {
            when(clientFactory.createAthenaClient(any())).thenReturn(athenaClient);
        }

        StartQueryExecutionRequest executeRequest() throws Exception {
            StartQueryExecutionResponse startQueryResponse = StartQueryExecutionResponse.builder().queryExecutionId("Q1234").build();
            when(athenaClient.startQueryExecution(ArgumentMatchers.<Consumer<StartQueryExecutionRequest.Builder>>any())).thenReturn(CompletableFuture.completedFuture(startQueryResponse));
            QueryExecutionStatus status = QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build();
            QueryExecution queryExecution = QueryExecution.builder().status(status).build();
            GetQueryExecutionResponse getQueryResponse = GetQueryExecutionResponse.builder().queryExecution(queryExecution).build();
            when(athenaClient.getQueryExecution(ArgumentMatchers.<Consumer<GetQueryExecutionRequest.Builder>>any())).thenReturn(CompletableFuture.completedFuture(getQueryResponse));
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

        @Nested
        class WhenTheRegionIsSetInTheEnvironment {
            @Test
            void readsThe_AWS_REGION_EnvironmentVariable() throws Exception {
                env.put("AWS_REGION", Region.AP_NORTHEAST_1.toString());
                defaultProperties.remove("AWS_REGION");
                driver.connect(jdbcUrl, defaultProperties);
                verify(clientFactory).createAthenaClient(Region.AP_NORTHEAST_1);
            }

            @Test
            void readsThe_AWS_DEFAULT_REGION_EnvironmentVariable() throws Exception {
                env.put("AWS_DEFAULT_REGION", Region.AP_NORTHEAST_2.toString());
                defaultProperties.remove("AWS_REGION");
                driver.connect(jdbcUrl, defaultProperties);
                verify(clientFactory).createAthenaClient(Region.AP_NORTHEAST_2);
            }

            @Test
            void uses_AWS_REGION_Over_AWS_DEFAULT_REGION() throws Exception {
                env.put("AWS_REGION", Region.AP_NORTHEAST_1.toString());
                env.put("AWS_DEFAULT_REGION", Region.AP_NORTHEAST_2.toString());
                defaultProperties.remove("AWS_REGION");
                driver.connect(jdbcUrl, defaultProperties);
                verify(clientFactory).createAthenaClient(Region.AP_NORTHEAST_1);
            }

            @Test
            void usesTheConfiguredRegionOverTheEnvironmentVariables() throws Exception {
                env.put("AWS_REGION", Region.AP_NORTHEAST_1.toString());
                env.put("AWS_DEFAULT_REGION", Region.AP_NORTHEAST_2.toString());
                defaultProperties.setProperty("AWS_REGION", Region.SA_EAST_1.toString());
                driver.connect(jdbcUrl, defaultProperties);
                verify(clientFactory).createAthenaClient(Region.SA_EAST_1);
            }
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
