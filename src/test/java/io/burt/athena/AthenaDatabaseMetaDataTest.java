package io.burt.athena;

import io.burt.athena.support.PomVersionLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AthenaDatabaseMetaDataTest implements PomVersionLoader {
    private DatabaseMetaData metaData;
    private Connection connection;

    @BeforeEach
    void setUp() {
        connection = mock(Connection.class);
        metaData = new AthenaDatabaseMetaData(connection);
    }

    @Nested
    class GetConnection {
        @Test
        void returnsTheConnectionTheMetaDataWasCreatedBy() throws Exception {
            assertSame(connection, metaData.getConnection());
        }
    }

    @Nested
    class IsWrapperFor {
        @Test
        void isWrapperForAthenaDatabaseMetaData() throws Exception {
            assertTrue(metaData.isWrapperFor(AthenaDatabaseMetaData.class));
        }

        @Test
        void isWrapperForObject() throws Exception {
            assertTrue(metaData.isWrapperFor(Object.class));
        }

        @Test
        void isNotWrapperForOtherClasses() throws Exception {
            assertFalse(metaData.isWrapperFor(String.class));
        }
    }

    @Nested
    class Unwrap {
        @Test
        void returnsTypedInstance() throws SQLException {
            AthenaDatabaseMetaData md = metaData.unwrap(AthenaDatabaseMetaData.class);
            assertNotNull(md);
        }

        @Test
        void throwsWhenAskedToUnwrapClassItIsNotWrapperFor() {
            assertThrows(SQLException.class, () -> metaData.unwrap(String.class));
        }
    }

    @Nested
    class GetDriverName {
        @Test
        void returnsTheDriverName() throws Exception {
            assertEquals("athena", metaData.getDriverName());
        }
    }

    @Nested
    class GetDriverVersion {
        @Test
        void returnsTheSameVersionAsInThePomFile() throws Exception {
            assertEquals(pomVersion().get(), metaData.getDriverVersion());
        }
    }

    @Nested
    class GetDriverMajorVersion {
        @Test
        void returnsTheSameMajorVersionAsInThePomFile() throws Exception {
            assertEquals(pomVersionComponents().get()[0], metaData.getDriverMajorVersion());
        }
    }

    @Nested
    class GetDriverMinorVersion {
        @Test
        void returnsTheSameMajorVersionAsInThePomFile() throws Exception {
            assertEquals(pomVersionComponents().get()[1], metaData.getDriverMinorVersion());
        }
    }

    @Nested
    class GetJdbcMajorVersion {
        @Test
        void returns4() throws Exception {
            assertEquals(4, metaData.getJDBCMajorVersion());
        }
    }

    @Nested
    class GetJdbcMinorVersion {
        @Test
        void returns4() throws Exception {
            assertEquals(2, metaData.getJDBCMinorVersion());
        }
    }

    @Nested
    class GetDatabaseProductName {
        @Test
        void returnsTheAwsProductName() throws Exception {
            assertEquals("Amazon Athena", metaData.getDatabaseProductName());
        }
    }

    @Nested
    class GetDatabaseProductVersion {
        @Test
        void returnsNull() throws Exception {
            assertNull(metaData.getDatabaseProductVersion());
        }
    }

    @Nested
    class GetDatabaseMajorVersion {
        @Test
        void returnsZero() throws Exception {
            assertEquals(0, metaData.getDatabaseMajorVersion());
        }
    }

    @Nested
    class GetDatabaseMinorVersion {
        @Test
        void returnsZero() throws Exception {
            assertEquals(0, metaData.getDatabaseMinorVersion());
        }
    }

    @Nested
    class GetUserName {
        @Test
        void returnsNull() throws Exception {
            assertNull(metaData.getUserName());
        }
    }

    @Nested
    class IsReadonly {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.isReadOnly());
        }
    }

    @Nested
    class GetURL {
        @BeforeEach
        void setUp() throws Exception {
            when(connection.getSchema()).thenReturn("some_db");
        }

        @Test
        void returnsAJdbcUrl() throws Exception {
            assertEquals(AthenaDriver.createURL("some_db"), metaData.getURL());
        }
    }

    @Nested
    class UsesLocalFiles {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.usesLocalFiles());
        }
    }

    @Nested
    class UsesLocalFilePerTable {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.usesLocalFilePerTable());
        }
    }
}