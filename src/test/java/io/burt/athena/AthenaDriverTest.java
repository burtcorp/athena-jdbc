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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
class AthenaDriverTest {
    @Mock private AwsClientFactory clientFactory;
    @Mock private AthenaAsyncClient athenaClient;

    private AthenaDriver driver;
    private Properties defaultProperties;
    private Map<String, String> env;

    @BeforeEach
    void setUpDriver() {
        env = new HashMap<>();
        driver = new AthenaDriver(clientFactory, env);
    }

    @BeforeEach
    void setUpProperties() {
        defaultProperties = new Properties();
        defaultProperties.setProperty(AthenaDriver.REGION_PROPERTY_NAME, Region.AP_SOUTHEAST_1.toString());
        defaultProperties.setProperty(AthenaDriver.WORK_GROUP_PROPERTY_NAME, "test_wg");
        defaultProperties.setProperty(AthenaDriver.OUTPUT_LOCATION_PROPERTY_NAME, "s3://test/location");
    }

    @Nested
    class Connect {
        @Captor ArgumentCaptor<Consumer<StartQueryExecutionRequest.Builder>> startQueryExecutionCaptor;

        private final String jdbcUrl = "jdbc:athena:test_db";

        @BeforeEach
        void setUpDriver() {
            when(clientFactory.createAthenaClient(any())).thenReturn(athenaClient);
        }

        StartQueryExecutionRequest executeRequest() throws Exception {
            return executeRequest(jdbcUrl);
        }

        StartQueryExecutionRequest executeRequest(String url) throws Exception {
            StartQueryExecutionResponse startQueryResponse = StartQueryExecutionResponse.builder().queryExecutionId("Q1234").build();
            when(athenaClient.startQueryExecution(ArgumentMatchers.<Consumer<StartQueryExecutionRequest.Builder>>any())).thenReturn(CompletableFuture.completedFuture(startQueryResponse));
            QueryExecutionStatus status = QueryExecutionStatus.builder().state(QueryExecutionState.SUCCEEDED).build();
            QueryExecution queryExecution = QueryExecution.builder().status(status).build();
            GetQueryExecutionResponse getQueryResponse = GetQueryExecutionResponse.builder().queryExecution(queryExecution).build();
            when(athenaClient.getQueryExecution(ArgumentMatchers.<Consumer<GetQueryExecutionRequest.Builder>>any())).thenReturn(CompletableFuture.completedFuture(getQueryResponse));
            Connection connection = driver.connect(url, defaultProperties);
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
        void parsesTheDatabaseNameFromTheUrl() throws Exception {
            StartQueryExecutionRequest request = executeRequest();
            assertEquals("test_db", request.queryExecutionContext().database());
        }

        @Test
        void usesTheDefaultDatabaseWhenThereIsNoDatabaseNameInTheUrl() throws Exception {
            StartQueryExecutionRequest request = executeRequest("jdbc:athena");
            assertEquals("default", request.queryExecutionContext().database());
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
                defaultProperties.remove(AthenaDriver.REGION_PROPERTY_NAME);
                driver.connect(jdbcUrl, defaultProperties);
                verify(clientFactory).createAthenaClient(Region.AP_NORTHEAST_1);
            }

            @Test
            void readsThe_AWS_DEFAULT_REGION_EnvironmentVariable() throws Exception {
                env.put("AWS_DEFAULT_REGION", Region.AP_NORTHEAST_2.toString());
                defaultProperties.remove(AthenaDriver.REGION_PROPERTY_NAME);
                driver.connect(jdbcUrl, defaultProperties);
                verify(clientFactory).createAthenaClient(Region.AP_NORTHEAST_2);
            }

            @Test
            void uses_AWS_REGION_Over_AWS_DEFAULT_REGION() throws Exception {
                env.put("AWS_REGION", Region.AP_NORTHEAST_1.toString());
                env.put("AWS_DEFAULT_REGION", Region.AP_NORTHEAST_2.toString());
                defaultProperties.remove(AthenaDriver.REGION_PROPERTY_NAME);
                driver.connect(jdbcUrl, defaultProperties);
                verify(clientFactory).createAthenaClient(Region.AP_NORTHEAST_1);
            }

            @Test
            void usesTheConfiguredRegionOverTheEnvironmentVariables() throws Exception {
                env.put("AWS_REGION", Region.AP_NORTHEAST_1.toString());
                env.put("AWS_DEFAULT_REGION", Region.AP_NORTHEAST_2.toString());
                defaultProperties.setProperty(AthenaDriver.REGION_PROPERTY_NAME, Region.SA_EAST_1.toString());
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
        void acceptsUrlWithDatabaseName() throws Exception {
            assertTrue(driver.acceptsURL("jdbc:athena:test_db"));
        }

        @Test
        void acceptsUrlWithoutDatabaseName() throws Exception {
            assertTrue(driver.acceptsURL("jdbc:athena"));
        }

        @Test
        void doesNotAcceptUrlWitEmptyDatabaseName() throws Exception {
            assertFalse(driver.acceptsURL("jdbc:athena:"));
        }

        @Test
        void doesNotAcceptUrlWithSlashes() throws Exception {
            assertFalse(driver.acceptsURL("jdbc:athena://default"));
        }

        @Test
        void doesNotAcceptUrlWithIllegalDatabaseName() throws Exception {
            assertFalse(driver.acceptsURL("jdbc:athena:1llegal_name"));
            assertFalse(driver.acceptsURL("jdbc:athena:$hit_name"));
        }

        @Test
        void doesNotAcceptNonAthenaUrl() throws Exception {
            assertFalse(driver.acceptsURL("jdbc:postgres://localhost/db"));
        }
    }

    @Nested
    class JdbcUrl {
        @Test
        void returnsAnAcceptableJdbcUrlForTheSpecifiedDatabase() throws Exception {
            assertTrue(driver.acceptsURL(AthenaDriver.jdbcUrl("test_db")));
            assertTrue(AthenaDriver.jdbcUrl("test_db").contains("test_db"));
            assertTrue(AthenaDriver.jdbcUrl("test_db").contains("jdbc:"));
            assertTrue(AthenaDriver.jdbcUrl("test_db").contains(AthenaDriver.JDBC_SUBPROTOCOL));
        }
    }

    @Nested
    class PropertyInfo {
        @Test
        void returnsEmptyPropertyInfo() throws Exception {
            assertEquals(0, driver.getPropertyInfo(AthenaDriver.jdbcUrl("test_db"), new Properties()).length);
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

    class SharedDriverManagerContext {
        Optional<Driver> findDriver() {
            for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements(); ) {
                Driver d = e.nextElement();
                if (d.getClass() == AthenaDriver.class) {
                    return Optional.of(d);
                }
            }
            return Optional.empty();
        }
    }

    @Nested
    class Register extends SharedDriverManagerContext {
        @Test
        void registersItselfWithTheGlobalDriverManager() throws Exception {
            AthenaDriver.register();
            assertTrue(findDriver().isPresent());
            assertNotNull(DriverManager.getDriver(AthenaDriver.jdbcUrl("default")));
        }
    }

    @Nested
    class Deregister extends SharedDriverManagerContext {
        @Test
        void deregistersItselfFromTheGlobalDriverManager() throws Exception {
            AthenaDriver.register();
            AthenaDriver.deregister();
            assertFalse(findDriver().isPresent());
        }
    }
}
