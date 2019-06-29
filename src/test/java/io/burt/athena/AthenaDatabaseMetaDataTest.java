package io.burt.athena;

import io.burt.athena.support.PomVersionLoader;
import io.burt.athena.support.TestNameGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
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
@DisplayNameGeneration(TestNameGenerator.class)
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

    @Nested
    class AllProceduresAreCallable {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.allProceduresAreCallable());
        }
    }

    @Nested
    class AllTablesAreSelectable {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.allTablesAreSelectable());
        }
    }

    @Nested
    class NullsAreSortedHigh {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.nullsAreSortedHigh());
        }
    }

    @Nested
    class NullsAreSortedLow {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.nullsAreSortedLow());
        }
    }

    @Nested
    class NullsAreSortedAtStart {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.nullsAreSortedAtStart());
        }
    }

    @Nested
    class NullsAreSortedAtEnd {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.nullsAreSortedAtEnd());
        }
    }

    @Nested
    class SupportsMixedCaseIdentifiers {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsMixedCaseIdentifiers());
        }
    }

    @Nested
    class StoresUpperCaseIdentifiers {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.storesUpperCaseIdentifiers());
        }
    }

    @Nested
    class StoresLowerCaseIdentifiers {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.storesLowerCaseIdentifiers());
        }
    }

    @Nested
    class StoresMixedCaseIdentifiers {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.storesMixedCaseIdentifiers());
        }
    }

    @Nested
    class SupportsMixedCaseQuotedIdentifiers {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsMixedCaseQuotedIdentifiers());
        }
    }

    @Nested
    class StoresUpperCaseQuotedIdentifiers {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.storesUpperCaseQuotedIdentifiers());
        }
    }

    @Nested
    class StoresLowerCaseQuotedIdentifiers {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.storesLowerCaseQuotedIdentifiers());
        }
    }

    @Nested
    class StoresMixedCaseQuotedIdentifiers {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.storesMixedCaseQuotedIdentifiers());
        }
    }

    @Nested
    class GetIdentifierQuoteString {
        @Test
        void returnsDoubleQuote() throws Exception {
            assertEquals("\"", metaData.getIdentifierQuoteString());
        }
    }

    @Nested
    class GetSqlKeywords {
        @Test
        void returnsAnEmptyString() throws Exception {
            assertEquals("", metaData.getSQLKeywords());
        }
    }

    @Nested
    class GetNumericFunctions {
        @Test
        void returnsAnEmptyString() throws Exception {
            assertEquals("", metaData.getNumericFunctions());
        }
    }

    @Nested
    class GetStringFunctions {
        @Test
        void returnsAnEmptyString() throws Exception {
            assertEquals("", metaData.getStringFunctions());
        }
    }

    @Nested
    class GetSystemFunctions {
        @Test
        void returnsAnEmptyString() throws Exception {
            assertEquals("", metaData.getSystemFunctions());
        }
    }

    @Nested
    class GetTimeDateFunctions {
        @Test
        void returnsAnEmptyString() throws Exception {
            assertEquals("", metaData.getTimeDateFunctions());
        }
    }

    @Nested
    class GetSearchStringEscape {
        @Test
        void returnsBackslash() throws Exception {
            assertEquals("\\", metaData.getSearchStringEscape());
        }
    }

    @Nested
    class GetExtraNameCharacters {
        @Test
        void returnsAnEmptyString() throws Exception {
            assertEquals("", metaData.getExtraNameCharacters());
        }
    }

    @Nested
    class SupportsAlterTableWithAddColumn {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsAlterTableWithAddColumn());
        }
    }

    @Nested
    class SupportsAlterTableWithDropColumn {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsAlterTableWithDropColumn());
        }
    }

    @Nested
    class SupportsColumnAliasing {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsColumnAliasing());
        }
    }

    @Nested
    class NullPlusNonNullIsNull {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.nullPlusNonNullIsNull());
        }
    }

    @Nested
    class SupportsConvert {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsConvert());
        }

        @Nested
        class WithTypes {
            // TODO
        }
    }

    @Nested
    class SupportsTableCorrelationNames {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsTableCorrelationNames());
        }
    }

    @Nested
    class SupportsDifferentTableCorrelationNames {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsDifferentTableCorrelationNames());
        }
    }

    @Nested
    class SupportsExpressionsInOrderBy {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsExpressionsInOrderBy());
        }
    }

    @Nested
    class SupportsOrderByUnrelated {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsOrderByUnrelated());
        }
    }

    @Nested
    class SupportsGroupBy {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsGroupBy());
        }
    }

    @Nested
    class SupportsGroupByUnrelated {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsGroupByUnrelated());
        }
    }

    @Nested
    class SupportsGroupByBeyondSelect {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsGroupByBeyondSelect());
        }
    }

    @Nested
    class SupportsLikeEscapeClause {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsLikeEscapeClause());
        }
    }

    @Nested
    class SupportsMultipleResultSets {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsMultipleResultSets());
        }
    }

    @Nested
    class SupportsMultipleTransactions {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsMultipleTransactions());
        }
    }

    @Nested
    class SupportsNonNullableColumns {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsNonNullableColumns());
        }
    }

    @Nested
    class SupportsMinimumSqlGrammar {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsMinimumSQLGrammar());
        }
    }

    @Nested
    class SupportsCoreSqlGrammar {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsCoreSQLGrammar());
        }
    }

    @Nested
    class SupportsExtendedSqlGrammar {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsExtendedSQLGrammar());
        }
    }

    @Nested
    class SupportsAnsi92EntryLevelSql {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsANSI92EntryLevelSQL());
        }
    }

    @Nested
    class SupportsAnsi92IntermediateSql {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsANSI92IntermediateSQL());
        }
    }

    @Nested
    class SupportsAnsi92FullSql {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsANSI92FullSQL());
        }
    }

    @Nested
    class SupportsIntegrityEnhancementFacility {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsIntegrityEnhancementFacility());
        }
    }

    @Nested
    class SupportsOuterJoins {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsOuterJoins());
        }
    }

    @Nested
    class SupportsFullOuterJoins {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsFullOuterJoins());
        }
    }

    @Nested
    class SupportsLimitedOuterJoins {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsLimitedOuterJoins());
        }
    }

    @Nested
    class GetSchemaTerm {
        @Test
        void returnsTheAthenaTerm() throws Exception {
            assertEquals("schema", metaData.getSchemaTerm());
        }
    }

    @Nested
    class GetProcedureTerm {
        @Test
        void returnsAnEmptyString() throws Exception {
            assertEquals("", metaData.getProcedureTerm());
        }
    }

    @Nested
    class GetCatalogTerm {
        @Test
        void returnsTheAthenaTerm() throws Exception {
            assertEquals("catalog", metaData.getCatalogTerm());
        }
    }

    @Nested
    class IsCatalogAtStart {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.isCatalogAtStart());
        }
    }

    @Nested
    class GetCatalogSeparator {
        @Test
        void returnsTheAthenaValue() throws Exception {
            assertEquals(".", metaData.getCatalogSeparator());
        }
    }

    @Nested
    class SupportsSchemasInDataManipulation {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsSchemasInDataManipulation());
        }
    }

    @Nested
    class SupportsSchemasInProcedureCalls {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsSchemasInProcedureCalls());
        }
    }

    @Nested
    class SupportsSchemasInTableDefinitions {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsSchemasInTableDefinitions());
        }
    }

    @Nested
    class SupportsSchemasInIndexDefinitions {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsSchemasInIndexDefinitions());
        }
    }

    @Nested
    class SupportsSchemasInPrivilegeDefinitions {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsSchemasInPrivilegeDefinitions());
        }
    }

    @Nested
    class SupportsCatalogsInDataManipulation {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsCatalogsInDataManipulation());
        }
    }

    @Nested
    class SupportsCatalogsInProcedureCalls {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsCatalogsInProcedureCalls());
        }
    }

    @Nested
    class SupportsCatalogsInTableDefinitions {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsCatalogsInTableDefinitions());
        }
    }

    @Nested
    class SupportsCatalogsInIndexDefinitions {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsSchemasInIndexDefinitions());
        }
    }

    @Nested
    class SupportsCatalogsInPrivilegeDefinitions {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsCatalogsInPrivilegeDefinitions());
        }
    }

    @Nested
    class SupportsPositionedDelete {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsPositionedDelete());
        }
    }

    @Nested
    class SupportsPositionedUpdate {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsPositionedUpdate());
        }
    }

    @Nested
    class SupportsSelectForUpdate {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsSelectForUpdate());
        }
    }

    @Nested
    class SupportsStoredProcedures {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsStoredProcedures());
        }
    }

    @Nested
    class SupportsSubqueriesInComparisons {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsSubqueriesInComparisons());
        }
    }

    @Nested
    class SupportsSubqueriesInExists {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsSubqueriesInExists());
        }
    }

    @Nested
    class SupportsSubqueriesInIns {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsSubqueriesInIns());
        }
    }

    @Nested
    class SupportsSubqueriesInQuantifieds {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsSubqueriesInQuantifieds());
        }
    }

    @Nested
    class SupportsCorrelatedSubqueries {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsCorrelatedSubqueries());
        }
    }

    @Nested
    class SupportsUnion {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsUnion());
        }
    }

    @Nested
    class SupportsUnionAll {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsUnionAll());
        }
    }

    @Nested
    class SupportsOpenCursorsAcrossCommit {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsOpenCursorsAcrossCommit());
        }
    }

    @Nested
    class SupportsOpenCursorsAcrossRollback {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsOpenCursorsAcrossRollback());
        }
    }

    @Nested
    class SupportsOpenStatementsAcrossCommit {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsOpenStatementsAcrossCommit());
        }
    }

    @Nested
    class SupportsOpenStatementsAcrossRollback {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.supportsOpenStatementsAcrossRollback());
        }
    }

    @Nested
    class GetMaxBinaryLiteralLength {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxBinaryLiteralLength());
        }
    }

    @Nested
    class GetMaxCharLiteralLength {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxCharLiteralLength());
        }
    }

    @Nested
    class GetMaxColumnNameLength {
        @Test
        void returnsGluesLimit() throws Exception {
            assertEquals(255, metaData.getMaxColumnNameLength());
        }
    }

    @Nested
    class GetMaxColumnsInGroupBy {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxColumnsInGroupBy());
        }
    }

    @Nested
    class GetMaxColumnsInIndex {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxColumnsInIndex());
        }
    }

    @Nested
    class GetMaxColumnsInOrderBy {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxColumnsInOrderBy());
        }
    }

    @Nested
    class GetMaxColumnsInSelect {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxColumnsInSelect());
        }
    }

    @Nested
    class GetMaxColumnsInTable {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxColumnsInTable());
        }
    }

    @Nested
    class GetMaxConnections {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxConnections());
        }
    }

    @Nested
    class GetMaxCursorNameLength {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxCursorNameLength());
        }
    }

    @Nested
    class GetMaxIndexLength {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxIndexLength());
        }
    }

    @Nested
    class GetMaxSchemaNameLength {
        @Test
        void returnsGluesLimit() throws Exception {
            assertEquals(255, metaData.getMaxSchemaNameLength());
        }
    }

    @Nested
    class GetMaxProcedureNameLength {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxProcedureNameLength());
        }
    }

    @Nested
    class GetMaxCatalogNameLength {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxCatalogNameLength());
        }
    }

    @Nested
    class GetMaxRowSize {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxRowSize());
        }
    }

    @Nested
    class DoesMaxRowSizeIncludeBlobs {
        @Test
        void returnsTrue() throws Exception {
            assertTrue(metaData.doesMaxRowSizeIncludeBlobs());
        }
    }

    @Nested
    class GetMaxStatementLength {
        @Test
        void returnsAthenasMaxQueryLimit() throws Exception {
            assertEquals(262144, metaData.getMaxStatementLength());
        }
    }

    @Nested
    class GetMaxStatements {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxStatements());
        }
    }

    @Nested
    class GetMaxTableNameLength {
        @Test
        void returnsGluesLimit() throws Exception {
            assertEquals(255, metaData.getMaxTableNameLength());
        }
    }

    @Nested
    class GetMaxTablesInSelect {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxTablesInSelect());
        }
    }

    @Nested
    class GetMaxUserNameLength {
        @Test
        void returnsNoLimit() throws Exception {
            assertEquals(0, metaData.getMaxUserNameLength());
        }
    }

    @Nested
    class GetDefaultTransactionIsolation {
        @Test
        void returnsNoTransactionLevel() throws Exception {
            assertEquals(Connection.TRANSACTION_NONE, metaData.getDefaultTransactionIsolation());
        }
    }

    @Nested
    class SupportsTransactions {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsTransactions());
        }
    }

    @Nested
    class SupportsTransactionIsolationLevel {
        @Test
        void returnsTrueForNone() throws Exception {
            assertTrue(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
        }

        @Test
        void returnsFalseForAllOtherLevels() throws Exception {
            assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
            assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
            assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
            assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
        }
    }

    @Nested
    class SupportsDataDefinitionAndDataManipulationTransactions {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsDataDefinitionAndDataManipulationTransactions());
        }
    }

    @Nested
    class SupportsDataManipulationTransactionsOnly {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsDataManipulationTransactionsOnly());
        }
    }

    @Nested
    class DataDefinitionCausesTransactionCommit {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.dataDefinitionCausesTransactionCommit());
        }
    }

    @Nested
    class DataDefinitionIgnoredInTransactions {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.dataDefinitionIgnoredInTransactions());
        }
    }

    @Nested
    class GetProcedures {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getProcedures("", "", ""));
        }
    }

    @Nested
    class GetProcedureColumns {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getProcedureColumns("", "", "", ""));
        }
    }

    @Nested
    class GetTables {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getTables("", "", "", new String[]{}));
        }
    }

    @Nested
    class GetSchemas {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getSchemas());
        }

        @Nested
        class WhenGivenAPattern {
            @Test
            void isNotSupported() {
                assertThrows(UnsupportedOperationException.class, () -> metaData.getSchemas("", ""));
            }
        }
    }

    @Nested
    class GetCatalogs {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getCatalogs());
        }
    }

    @Nested
    class GetTableTypes {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getTableTypes());
        }
    }

    @Nested
    class GetColumns {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getColumns("", "", "", ""));
        }
    }

    @Nested
    class GetColumnPrivileges {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getColumnPrivileges("", "", "", ""));
        }
    }

    @Nested
    class GetTablePrivileges {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getTablePrivileges("", "", ""));
        }
    }

    @Nested
    class GetBestRowIdentifier {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getBestRowIdentifier("", "", "", 0, false));
        }
    }

    @Nested
    class GetVersionColumns {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getVersionColumns("", "", ""));
        }
    }

    @Nested
    class GetPrimaryKeys {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getPrimaryKeys("", "", ""));
        }
    }

    @Nested
    class GetImportedKeys {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getImportedKeys("", "", ""));
        }
    }

    @Nested
    class GetExportedKeys {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getExportedKeys("", "", ""));
        }
    }

    @Nested
    class GetCrossReference {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getCrossReference("", "", "", "", "", ""));
        }
    }

    @Nested
    class GetTypeInfo {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getTypeInfo());
        }
    }

    @Nested
    class GetIndexInfo {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getIndexInfo("", "", "", false, false));
        }
    }

    @Nested
    class SupportsResultSetType {
        @Nested
        class WhenGivenForwardOnly {
            @Test
            void returnsTrue() throws Exception {
                assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
            }
        }

        @Nested
        class WhenGivenScrollInsensitive {
            @Test
            void returnsFalse() throws Exception {
                assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
            }
        }

        @Nested
        class WhenGivenScrollSensitive {
            @Test
            void returnsFalse() throws Exception {
                assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
            }
        }
    }

    @Nested
    class SupportsResultSetConcurrency {
        @Nested
        class WhenTypeIsForwardOnly {
            @Nested
            class WhenGivenReadOnly {
                @Test
                void returnsTrue() throws Exception {
                    assertTrue(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
                }
            }

            @Nested
            class WhenGivenUpdatable {
                @Test
                void returnsFalse() throws Exception {
                    assertFalse(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
                }
            }
        }

        @Nested
        class WhenTypeIsNotForwardOnly {
            @Test
            void returnsFalse() throws Exception {
                assertFalse(metaData.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
            }
        }
    }

    @Nested
    class OwnUpdatesAreVisible {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class OwnDeletesAreVisible {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class OwnInsertsAreVisible {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.ownInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class OthersUpdatesAreVisible {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class OthersDeletesAreVisible {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class OthersInsertsAreVisible {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class UpdatesAreDetected {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.updatesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class DeletesAreDetected {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class InsertsAreDetected {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY));
            assertFalse(metaData.insertsAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE));
            assertFalse(metaData.insertsAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    @Nested
    class SupportsBatchUpdates {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsBatchUpdates());
        }
    }

    @Nested
    class GetUdts {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getUDTs("", "", "", new int[]{}));
        }
    }

    @Nested
    class SupportsSavepoints {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsSavepoints());
        }
    }

    @Nested
    class SupportsNamedParameters {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsNamedParameters());
        }
    }

    @Nested
    class SupportsMultipleOpenResults {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsMultipleOpenResults());
        }
    }

    @Nested
    class SupportsGetGeneratedKeys {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsGetGeneratedKeys());
        }
    }

    @Nested
    class GetSuperTypes {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getSuperTypes("", "", ""));
        }
    }

    @Nested
    class GetSuperTables {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getSuperTables("", "", ""));
        }
    }

    @Nested
    class GetAttributes {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getAttributes("", "", "", ""));
        }
    }

    @Nested
    class SupportsResultSetHoldability {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
            assertFalse(metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
        }
    }

    @Nested
    class GetResultSetHoldability {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getResultSetHoldability());
        }
    }

    @Nested
    class GetSqlStateType {
        @Test
        void returnsSql() throws Exception {
            assertEquals(DatabaseMetaData.sqlStateSQL, metaData.getSQLStateType());
        }
    }

    @Nested
    class LocatorsUpdateCopy {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.locatorsUpdateCopy());
        }
    }

    @Nested
    class SupportsStatementPooling {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsStatementPooling());
        }
    }

    @Nested
    class GetRowIdLifetime {
        @Test
        void returnsUpsupported() throws Exception {
            assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, metaData.getRowIdLifetime());
        }
    }

    @Nested
    class SupportsStoredFunctionsUsingCallSyntax {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.supportsStoredFunctionsUsingCallSyntax());
        }
    }

    @Nested
    class AutoCommitFailureClosesAllResultSets {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.autoCommitFailureClosesAllResultSets());
        }
    }

    @Nested
    class GetClientInfoProperties {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getClientInfoProperties());
        }
    }

    @Nested
    class GetFunctions {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getFunctions("", "", ""));
        }
    }

    @Nested
    class GetFunctionColumns {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getFunctionColumns("", "", "", ""));
        }
    }

    @Nested
    class GetPseudoColumns {
        @Test
        void isNotSupported() {
            assertThrows(UnsupportedOperationException.class, () -> metaData.getPseudoColumns("", "", "", ""));
        }
    }

    @Nested
    class GeneratedKeyAlwaysReturned {
        @Test
        void returnsFalse() throws Exception {
            assertFalse(metaData.generatedKeyAlwaysReturned());
        }
    }
}
