package io.burt.athena;

import io.burt.athena.support.PomVersionLoader;
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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;

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
class AthenaDriverTest implements PomVersionLoader {
    private AthenaDriver driver;
    private Properties defaultProperties;
    private QueryExecutionHelper queryExecutionHelper;
    private ConnectionConfigurationFactory connectionConfigurationFactory;

    @BeforeEach
    void setUpDriver() {
        connectionConfigurationFactory = spy(new ConnectionConfigurationFactory());
        lenient().when(connectionConfigurationFactory.createConnectionConfiguration(any(), any(), any(), any(), any(), any())).then(invocation -> {
            ConnectionConfiguration cc = (ConnectionConfiguration) invocation.callRealMethod();
            cc = spy(cc);
            lenient().when(cc.athenaClient()).thenReturn(queryExecutionHelper);
            return cc;
        });
        driver = new AthenaDriver(connectionConfigurationFactory);
        queryExecutionHelper = new QueryExecutionHelper();
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
        @BeforeEach
        void setUp() {
            queryExecutionHelper.queueStartQueryResponse("Q1234");
            queryExecutionHelper.queueGetQueryExecutionResponse(QueryExecutionState.SUCCEEDED);
        }

        @Test
        void returnsConnection() {
            assertNotNull(driver.connect("jdbc:athena:test_db", defaultProperties));
        }

        @Test
        void parsesTheDatabaseNameFromTheUrl() throws Exception {
            driver.connect("jdbc:athena:test_db", defaultProperties).createStatement().execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("test_db", request.queryExecutionContext().database());
        }

        @Test
        void usesTheDefaultDatabaseWhenThereIsNoDatabaseNameInTheUrl() throws Exception {
            driver.connect("jdbc:athena", defaultProperties).createStatement().execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("default", request.queryExecutionContext().database());
        }

        @Test
        void usesTheAwsRegionFromTheProperties() {
            driver.connect("jdbc:athena", defaultProperties);
            verify(connectionConfigurationFactory).createConnectionConfiguration(eq(Region.AP_SOUTHEAST_1), any(), any(), any(), any(), any());
        }

        @Test
        void usesTheWorkGroupFromTheProperties() throws Exception {
            driver.connect("jdbc:athena:test_db", defaultProperties).createStatement().execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("test_wg", request.workGroup());
        }

        @Test
        void usesTheOutputLocationFromTheProperties() throws Exception {
            driver.connect("jdbc:athena:test_db", defaultProperties).createStatement().execute("SELECT 1");
            StartQueryExecutionRequest request = queryExecutionHelper.startQueryRequests().get(0);
            assertEquals("s3://test/location", request.resultConfiguration().outputLocation());
        }

        @Nested
        class WhenGivenABadUrl {
            @Test
            void returnsNull() {
                assertNull(driver.connect("athena:jdbc://hello", new Properties()));
            }
        }
    }

    @Nested
    class AcceptsUrl {
        @Test
        void acceptsUrlWithDatabaseName() {
            assertTrue(driver.acceptsURL("jdbc:athena:test_db"));
        }

        @Test
        void acceptsUrlWithoutDatabaseName() {
            assertTrue(driver.acceptsURL("jdbc:athena"));
        }

        @Test
        void doesNotAcceptUrlWitEmptyDatabaseName() {
            assertFalse(driver.acceptsURL("jdbc:athena:"));
        }

        @Test
        void doesNotAcceptUrlWithSlashes() {
            assertFalse(driver.acceptsURL("jdbc:athena://default"));
        }

        @Test
        void doesNotAcceptUrlWithIllegalDatabaseName() {
            assertFalse(driver.acceptsURL("jdbc:athena:1llegal_name"));
            assertFalse(driver.acceptsURL("jdbc:athena:$hit_name"));
        }

        @Test
        void doesNotAcceptNonAthenaUrl() {
            assertFalse(driver.acceptsURL("jdbc:postgres://localhost/db"));
        }
    }

    @Nested
    class JdbcUrl {
        @Test
        void returnsAnAcceptableJdbcUrlForTheSpecifiedDatabase() {
            assertTrue(driver.acceptsURL(AthenaDriver.createURL("test_db")));
            assertTrue(AthenaDriver.createURL("test_db").contains("test_db"));
            assertTrue(AthenaDriver.createURL("test_db").contains("jdbc:"));
            assertTrue(AthenaDriver.createURL("test_db").contains(AthenaDriver.JDBC_SUBPROTOCOL));
        }
    }

    @Nested
    class PropertyInfo {
        @Test
        void returnsEmptyPropertyInfo() {
            assertEquals(0, driver.getPropertyInfo(AthenaDriver.createURL("test_db"), new Properties()).length);
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
            assertNotNull(DriverManager.getDriver(AthenaDriver.createURL("default")));
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

    @Nested
    class GetMajorVersion {
        @Test
        void returnsTheSameMajorVersionAsInThePomFile() throws Exception {
            assertEquals(pomVersionComponents().get()[0], driver.getMajorVersion());
        }
    }

    @Nested
    class GetMinorVersion {
        @Test
        void returnsTheSameMajorVersionAsInThePomFile() throws Exception {
            assertEquals(pomVersionComponents().get()[1], driver.getMinorVersion());
        }
    }
}
