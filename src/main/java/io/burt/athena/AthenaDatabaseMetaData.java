package io.burt.athena;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

class AthenaDatabaseMetaData implements DatabaseMetaData {
    private final Connection connection;

    AthenaDatabaseMetaData(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return iface.cast(this);
        } else {
            throw new SQLException(String.format("%s is not a wrapper for %s", this.getClass().getName(), iface.getName()));
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public String getDriverName() {
        return AthenaDriver.JDBC_SUBPROTOCOL;
    }

    @Override
    public String getDriverVersion() {
        return "0.2.0-SNAPSHOT";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 2;
    }

    @Override
    public String getDatabaseProductName() {
        return "Amazon Athena";
    }

    @Override
    public String getDatabaseProductVersion() {
        return null;
    }

    @Override
    public int getDatabaseMajorVersion() {
        return 0;
    }

    @Override
    public int getDatabaseMinorVersion() {
        return 0;
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 2;
    }

    @Override
    public String getUserName() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return AthenaDriver.createURL(connection.getSchema());
    }

    @Override
    public boolean usesLocalFiles() {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() {
        return false;
    }

    @Override
    public boolean allProceduresAreCallable() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean allTablesAreSelectable() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean nullsAreSortedHigh() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean nullsAreSortedLow() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getIdentifierQuoteString() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getSQLKeywords() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getNumericFunctions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getStringFunctions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getSystemFunctions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getTimeDateFunctions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getSearchStringEscape() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getExtraNameCharacters() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsColumnAliasing() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsConvert() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsGroupBy() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsMultipleResultSets() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsMultipleTransactions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsNonNullableColumns() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsOuterJoins() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsFullOuterJoins() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getSchemaTerm() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getProcedureTerm() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getCatalogTerm() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isCatalogAtStart() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getCatalogSeparator() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsPositionedDelete() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsPositionedUpdate() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSelectForUpdate() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsStoredProcedures() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsUnion() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsUnionAll() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxCharLiteralLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxColumnNameLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxColumnsInIndex() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxColumnsInSelect() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxColumnsInTable() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxConnections() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxCursorNameLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxIndexLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxSchemaNameLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxProcedureNameLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxCatalogNameLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxRowSize() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxStatementLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxStatements() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxTableNameLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxTablesInSelect() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getMaxUserNameLength() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getDefaultTransactionIsolation() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsTransactions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getSchemas() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getCatalogs() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getTableTypes() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)  {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getTypeInfo() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsResultSetType(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean updatesAreDetected(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean deletesAreDetected(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean insertsAreDetected(int type) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsBatchUpdates() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSavepoints() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsNamedParameters() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getResultSetHoldability() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getSQLStateType() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean locatorsUpdateCopy() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsStatementPooling() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getClientInfoProperties() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
