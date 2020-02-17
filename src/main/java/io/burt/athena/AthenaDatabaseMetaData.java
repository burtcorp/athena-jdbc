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
        return AthenaDriverInfo.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return AthenaDriverInfo.getDriverMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return AthenaDriverInfo.getDriverMinorVersion();
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
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh() {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() {
        return true;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() {
        return true;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() {
        return "\"";
    }

    @Override
    public String getSQLKeywords() {
        return "";
    }

    @Override
    public String getNumericFunctions() {
        return "";
    }

    @Override
    public String getStringFunctions() {
        return "";
    }

    @Override
    public String getSystemFunctions() {
        return "";
    }

    @Override
    public String getTimeDateFunctions() {
        return "";
    }

    @Override
    public String getSearchStringEscape() {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    @Override
    public boolean supportsConvert() {
        return true;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsTableCorrelationNames() {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() {
        return true;
    }

    @Override
    public boolean supportsGroupBy() {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() {
        return true;
    }

    @Override
    public boolean supportsNonNullableColumns() {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    @Override
    public String getSchemaTerm() {
        return "schema";
    }

    @Override
    public String getProcedureTerm() {
        return "";
    }

    @Override
    public String getCatalogTerm() {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() {
        return true;
    }

    @Override
    public String getCatalogSeparator() {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    @Override
    public boolean supportsUnion() {
        return true;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() {
        return true;
    }

    @Override
    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() {
        return 255;
    }

    @Override
    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() {
        return 0;
    }

    @Override
    public int getMaxConnections() {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() {
        return 0;
    }

    @Override
    public int getMaxIndexLength() {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() {
        return 255;
    }

    @Override
    public int getMaxProcedureNameLength() {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() {
        return 0;
    }

    @Override
    public int getMaxRowSize() {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    @Override
    public int getMaxStatementLength() {
        return 262144;
    }

    @Override
    public int getMaxStatements() {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() {
        return 255;
    }

    @Override
    public int getMaxTablesInSelect() {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
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
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
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
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsSavepoints() {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
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
        return false;
    }

    @Override
    public int getResultSetHoldability() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int getSQLStateType() {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
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
        return false;
    }
}
