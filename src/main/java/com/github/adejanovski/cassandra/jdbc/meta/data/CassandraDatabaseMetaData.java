/*
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.github.adejanovski.cassandra.jdbc.meta.data;

import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NOT_SUPPORTED;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NO_INTERFACE;

import com.datastax.driver.core.Metadata;
import com.github.adejanovski.cassandra.jdbc.CassandraDriver;
import com.github.adejanovski.cassandra.jdbc.connection.CassandraConnection;
import com.github.adejanovski.cassandra.jdbc.result.set.CassandraResultSet;
import com.github.adejanovski.cassandra.jdbc.result.set.MetadataResultSets;
import com.github.adejanovski.cassandra.jdbc.statement.CassandraStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;

public class CassandraDatabaseMetaData implements DatabaseMetaData {

  private CassandraConnection connection;
  private CassandraStatement statement;
  private Metadata metadata;

  public CassandraDatabaseMetaData(CassandraConnection connection) throws SQLException {
    this.connection = connection;
    this.statement = new CassandraStatement(this.connection);
    this.metadata = this.connection.getClusterMetadata();
  }

  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException {
      if (iface.isAssignableFrom(getClass())) {
          return iface.cast(this);
      }
    throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
  }

  public boolean allProceduresAreCallable() {
    return false;
  }

  public boolean allTablesAreSelectable() {
    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() {
    return false;
  }

  public boolean dataDefinitionCausesTransactionCommit() {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  public boolean deletesAreDetected(int arg0) {
    return false;
  }

  public boolean doesMaxRowSizeIncludeBlobs() {
    return false;
  }

  public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) {
    return new CassandraResultSet();
  }

  public ResultSet getBestRowIdentifier(String arg0, String arg1, String arg2, int arg3, boolean arg4) {
    return new CassandraResultSet();
  }

  public String getCatalogSeparator() {
    return "";
  }

  public String getCatalogTerm() {
    return "Cluster";
  }

  public ResultSet getCatalogs() throws SQLException {
    if (statement.isClosed()) {
      statement = new CassandraStatement(this.connection);
    }
    return MetadataResultSets.instance.makeCatalogs(statement);

  }

  public ResultSet getClientInfoProperties() {
    return new CassandraResultSet();
  }

  public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2, String arg3) {
    return new CassandraResultSet();
  }

  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    if (statement.isClosed()) {
      statement = new CassandraStatement(this.connection);
    }
    if (catalog == null || connection.getCatalog().equals(catalog)) {
      statement.connection = connection;
        if (schemaPattern == null) {
            schemaPattern = connection.getSchema(); //limit to current schema if set
        }
      return MetadataResultSets.instance.makeColumns(statement, schemaPattern, tableNamePattern, columnNamePattern);
    }
    return new CassandraResultSet();
  }

  public Connection getConnection() {
    return connection;
  }

  public ResultSet getCrossReference(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5) {
    return new CassandraResultSet();
  }

  public int getDatabaseMajorVersion() {
    return CassandraConnection.DB_MAJOR_VERSION;
  }

  public int getDatabaseMinorVersion() {
    return CassandraConnection.DB_MINOR_VERSION;
  }

  public String getDatabaseProductName() {
    return CassandraConnection.DB_PRODUCT_NAME;
  }

  public String getDatabaseProductVersion() {
    return String.format("%d.%d.%d", CassandraConnection.DB_MAJOR_VERSION, CassandraConnection.DB_MINOR_VERSION, CassandraConnection.DB_REVISION);
  }

  public int getDefaultTransactionIsolation() {
    return Connection.TRANSACTION_NONE;
  }

  public int getDriverMajorVersion() {
    return CassandraDriver.DVR_MAJOR_VERSION;
  }

  public int getDriverMinorVersion() {
    return CassandraDriver.DVR_MINOR_VERSION;
  }

  public String getDriverName() {
    return CassandraDriver.DVR_NAME;
  }

  public String getDriverVersion() {
    return String.format("%d.%d.%d", CassandraDriver.DVR_MAJOR_VERSION, CassandraDriver.DVR_MINOR_VERSION, CassandraDriver.DVR_PATCH_VERSION);
  }

  public ResultSet getExportedKeys(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public String getExtraNameCharacters() {
    return "";
  }

  public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) {
    return new CassandraResultSet();
  }

  public ResultSet getFunctions(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public String getIdentifierQuoteString() {
    return " ";
  }

  public ResultSet getImportedKeys(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    if (statement.isClosed()) {
      statement = new CassandraStatement(this.connection);
    }
    if (catalog == null || connection.getCatalog().equals(catalog)) {
        if (schema == null) {
            schema = connection.getSchema(); //limit to current schema if set
        }
      return MetadataResultSets.instance.makeIndexes(statement, schema, table, unique, approximate);
    }
    return new CassandraResultSet();
  }

  public int getJDBCMajorVersion() {
    return 4;
  }

  public int getJDBCMinorVersion() {
    return 0;
  }

  public int getMaxBinaryLiteralLength() {
    // Cassandra can represent a 2GB value, but CQL has to encode it in hex
    return Integer.MAX_VALUE / 2;
  }

  public int getMaxCatalogNameLength() {
    return Short.MAX_VALUE;
  }

  public int getMaxCharLiteralLength() {
    return Integer.MAX_VALUE;
  }

  public int getMaxColumnNameLength() {
    return Short.MAX_VALUE;
  }

  public int getMaxColumnsInGroupBy() {
    return 0;
  }

  public int getMaxColumnsInIndex() {
    return 0;
  }

  public int getMaxColumnsInOrderBy() {
    return 0;
  }

  public int getMaxColumnsInSelect() {
    return 0;
  }

  public int getMaxColumnsInTable() {
    return 0;
  }

  public int getMaxConnections() {
    return 0;
  }

  public int getMaxCursorNameLength() {
    return 0;
  }

  public int getMaxIndexLength() {
    return 0;
  }

  public int getMaxProcedureNameLength() {
    return 0;
  }

  public int getMaxRowSize() {
    return 0;
  }

  public int getMaxSchemaNameLength() {
    return 0;
  }

  public int getMaxStatementLength() {
    return 0;
  }

  public int getMaxStatements() {
    return 0;
  }

  public int getMaxTableNameLength() {
    return 0;
  }

  public int getMaxTablesInSelect() {
    return 0;
  }

  public int getMaxUserNameLength() {
    return 0;
  }

  public String getNumericFunctions() {
    return "";
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    if (statement.isClosed()) {
      statement = new CassandraStatement(this.connection);
    }
    if (catalog == null || connection.getCatalog().equals(catalog)) {
        if (schema == null) {
            schema = connection.getSchema(); //limit to current schema if set
        }
      return MetadataResultSets.instance.makePrimaryKeys(statement, schema, table);
    }
    return new CassandraResultSet();
  }

  public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3) {
    return new CassandraResultSet();
  }

  public String getProcedureTerm() {
    return "";
  }

  public ResultSet getProcedures(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public int getResultSetHoldability() {
    return CassandraResultSet.DEFAULT_HOLDABILITY;
  }

  public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_VALID_FOREVER;
  }

  public String getSQLKeywords() {
    return "";
  }

  public int getSQLStateType() {
    return sqlStateSQL;
  }

  public String getSchemaTerm() {
    return "Column Family";
  }

  public ResultSet getSchemas() throws SQLException {
    if (statement.isClosed())
      statement = new CassandraStatement(this.connection);
    return MetadataResultSets.instance.makeSchemas(statement, null);


  }

  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    if (statement.isClosed()) {
      statement = new CassandraStatement(this.connection);
    }
    if (!(catalog == null || catalog.equals(statement.connection.getCatalog()))) {
      throw new SQLSyntaxErrorException("catalog name must exactly match or be null");
    }

    return MetadataResultSets.instance.makeSchemas(statement, schemaPattern);

    //return new CassandraResultSet();
  }

  public String getSearchStringEscape() {
    return "\\";
  }

  public String getStringFunctions() {
    return "";
  }

  public ResultSet getSuperTables(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public ResultSet getSuperTypes(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public String getSystemFunctions() {
    return "";
  }

  public ResultSet getTablePrivileges(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public ResultSet getTableTypes() throws SQLException {
    if (statement.isClosed()) {
      statement = new CassandraStatement(this.connection);
    }
    return MetadataResultSets.instance.makeTableTypes(statement);
  }

  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {

    boolean askingForTable = (types == null);
    if (types != null) {
      for (String t : types) {
        if (MetadataResultSets.TABLE_CONSTANT.equals(t)) {
          askingForTable = true;
          break;
        }
      }
    }
    if ((catalog == null || connection.getCatalog().equals(catalog)) && askingForTable) {
      if (statement.isClosed()) {
        statement = new CassandraStatement(this.connection);
      }
      //if (schemaPattern == null) schemaPattern = connection.getSchema(); //limit to current schema if set
      return MetadataResultSets.instance.makeTables(statement, schemaPattern, tableNamePattern);
    }

    return new CassandraResultSet();
  }

  public String getTimeDateFunctions() {
    return "";
  }

  public ResultSet getTypeInfo() {
    return new CassandraResultSet();
  }

  public ResultSet getUDTs(String arg0, String arg1, String arg2, int[] arg3) {
    return new CassandraResultSet();
  }

  public String getURL() {
    return connection.url;
  }

  public String getUserName() {
    return (connection.username == null) ? "" : connection.username;
  }

  public ResultSet getVersionColumns(String arg0, String arg1, String arg2) {
    return new CassandraResultSet();
  }

  public boolean insertsAreDetected(int arg0) {
    return false;
  }

  public boolean isCatalogAtStart() {
    return false;
  }

  public boolean isReadOnly() {
    return false;
  }

  public boolean locatorsUpdateCopy() {
    return false;
  }

  public boolean nullPlusNonNullIsNull() {
    return false;
  }

  public boolean nullsAreSortedAtEnd() {
    return false;
  }

  public boolean nullsAreSortedAtStart() {
    return true;
  }

  public boolean nullsAreSortedHigh() {
    return true;
  }

  public boolean nullsAreSortedLow() {

    return false;
  }

  public boolean othersDeletesAreVisible(int arg0) {
    return false;
  }

  public boolean othersInsertsAreVisible(int arg0) {
    return false;
  }

  public boolean othersUpdatesAreVisible(int arg0) {
    return false;
  }

  public boolean ownDeletesAreVisible(int arg0) {
    return false;
  }

  public boolean ownInsertsAreVisible(int arg0) {
    return false;
  }

  public boolean ownUpdatesAreVisible(int arg0) {
    return false;
  }

  public boolean storesLowerCaseIdentifiers() {
    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() {
    return false;
  }

  public boolean storesMixedCaseIdentifiers() {
    return true;
  }

  public boolean storesMixedCaseQuotedIdentifiers() {
    return true;
  }

  public boolean storesUpperCaseIdentifiers() {
    return false;
  }

  public boolean storesUpperCaseQuotedIdentifiers() {
    return false;
  }

  public boolean supportsANSI92EntryLevelSQL() {
    return false;
  }

  public boolean supportsANSI92FullSQL() {
    return false;
  }

  public boolean supportsANSI92IntermediateSQL() {
    return false;
  }

  public boolean supportsAlterTableWithAddColumn() {
    return true;
  }

  public boolean supportsAlterTableWithDropColumn() {
    return true;
  }

  public boolean supportsBatchUpdates() {
    return false;
  }

  public boolean supportsCatalogsInDataManipulation() {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() {
    return false;
  }

  public boolean supportsColumnAliasing() {
    return false;
  }

  public boolean supportsConvert() {
    return false;
  }

  public boolean supportsConvert(int arg0, int arg1) {
    return false;
  }

  public boolean supportsCoreSQLGrammar() {
    return false;
  }

  public boolean supportsCorrelatedSubqueries() {
    return false;
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    return false;
  }

  public boolean supportsDataManipulationTransactionsOnly() {
    return false;
  }

  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  public boolean supportsExpressionsInOrderBy() {
    return false;
  }

  public boolean supportsExtendedSQLGrammar() {
    return false;
  }

  public boolean supportsFullOuterJoins() {
    return false;
  }

  public boolean supportsGetGeneratedKeys() {
    return false;
  }

  public boolean supportsGroupBy() {
    return false;
  }

  public boolean supportsGroupByBeyondSelect() {
    return false;
  }

  public boolean supportsGroupByUnrelated() {
    return false;
  }

  public boolean supportsIntegrityEnhancementFacility() {
    return false;
  }

  public boolean supportsLikeEscapeClause() {

    return false;
  }

  public boolean supportsLimitedOuterJoins() {
    return false;
  }

  public boolean supportsMinimumSQLGrammar() {
    return false;
  }

  public boolean supportsMixedCaseIdentifiers() {
    return true;
  }

  public boolean supportsMixedCaseQuotedIdentifiers() {
    return true;
  }

  public boolean supportsMultipleOpenResults() {
    return false;
  }

  public boolean supportsMultipleResultSets() {
    return false;
  }

  public boolean supportsMultipleTransactions() {
    return false;
  }

  public boolean supportsNamedParameters() {
    return false;
  }

  public boolean supportsNonNullableColumns() {

    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() {
    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() {
    return false;
  }

  public boolean supportsOpenStatementsAcrossCommit() {
    return false;
  }

  public boolean supportsOpenStatementsAcrossRollback() {
    return false;
  }

  public boolean supportsOrderByUnrelated() {
    return false;
  }

  public boolean supportsOuterJoins() {
    return false;
  }

  public boolean supportsPositionedDelete() {
    return false;
  }

  public boolean supportsPositionedUpdate() {
    return false;
  }

  public boolean supportsResultSetConcurrency(int arg0, int arg1) {
    return false;
  }

  public boolean supportsResultSetHoldability(int holdability) {

    return ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability;
  }

  public boolean supportsResultSetType(int type) {

    return ResultSet.TYPE_FORWARD_ONLY == type;
  }

  public boolean supportsSavepoints() {
    return false;
  }

  public boolean supportsSchemasInDataManipulation() {
    return true;
  }

  public boolean supportsSchemasInIndexDefinitions() {
    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() {
    return false;
  }

  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  public boolean supportsSchemasInTableDefinitions() {
    return false;
  }

  public boolean supportsSelectForUpdate() {
    return false;
  }

  public boolean supportsStatementPooling() {
    return false;
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() {
    return false;
  }

  public boolean supportsStoredProcedures() {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() {
    return false;
  }

  public boolean supportsSubqueriesInExists() {
    return false;
  }

  public boolean supportsSubqueriesInIns() {
    return false;
  }

  public boolean supportsSubqueriesInQuantifieds() {
    return false;
  }

  public boolean supportsTableCorrelationNames() {
    return false;
  }

  public boolean supportsTransactionIsolationLevel(int level) {

    return Connection.TRANSACTION_NONE == level;
  }

  public boolean supportsTransactions() {
    return false;
  }

  public boolean supportsUnion() {
    return false;
  }

  public boolean supportsUnionAll() {
    return false;
  }

  public boolean updatesAreDetected(int arg0) {
    return false;
  }

  public boolean usesLocalFilePerTable() {
    return false;
  }

  public boolean usesLocalFiles() {
    return false;
  }

  public boolean generatedKeyAlwaysReturned() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }
}
