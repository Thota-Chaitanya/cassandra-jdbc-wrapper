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
package com.github.adejanovski.cassandra.jdbc.connection;

import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NO_INTERFACE;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.WAS_CLOSED_CON;

import com.github.adejanovski.cassandra.jdbc.statement.ManagedPreparedStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ManagedConnection extends AbstractConnection implements Connection {

  private PooledCassandraConnection pooledCassandraConnection;

  private CassandraConnection physicalConnection;

  private Set<Statement> statements = new HashSet<>();

  public ManagedConnection(PooledCassandraConnection pooledCassandraConnection) {
    this.pooledCassandraConnection = pooledCassandraConnection;
    this.physicalConnection = pooledCassandraConnection.getConnection();
  }

  private void checkNotClosed() throws SQLNonTransientConnectionException {
    if (isClosed()) {
      throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
    }
  }

  @Override
  public boolean isClosed() {
    return physicalConnection == null;
  }

  @Override
  public synchronized void close() throws SQLException {
    for (Statement statement : statements) {
      if (!statement.isClosed()) {
        statement.close();
      }
    }
    pooledCassandraConnection.connectionClosed();
    pooledCassandraConnection = null;
    physicalConnection = null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  @Override
  public Statement createStatement() throws SQLException {
    checkNotClosed();
    Statement statement = physicalConnection.createStatement();
    statements.add(statement);
    return statement;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    checkNotClosed();
    Statement statement = physicalConnection.createStatement(resultSetType, resultSetConcurrency);
    statements.add(statement);
    return statement;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkNotClosed();
    Statement statement = physicalConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    statements.add(statement);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String cql) throws SQLException {
    checkNotClosed();
    ManagedPreparedStatement statement = pooledCassandraConnection.prepareStatement(this, cql);
    statements.add(statement);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  @Override
  public boolean isValid(int timeout) throws SQLTimeoutException {
    return physicalConnection.isValid(timeout);
  }

  @Override
  public void setClientInfo(String name, String value) {
    if (!isClosed()) {
      physicalConnection.setClientInfo(name, value);
    }
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.nativeSQL(sql);
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  // all following methods have the form
  // checkNotClosed-try-return/void-catch-notify-throw

  @Override
  public boolean getAutoCommit() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getAutoCommit();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.setAutoCommit(autoCommit);
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void commit() throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.commit();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void rollback() throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.rollback();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getMetaData();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.isReadOnly();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.setReadOnly(readOnly);
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public String getCatalog() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getCatalog();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.setCatalog(catalog);
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getTransactionIsolation();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.setTransactionIsolation(level);
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getWarnings();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.clearWarnings();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getHoldability();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    checkNotClosed();
    try {
      physicalConnection.setHoldability(holdability);
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getClientInfo(name);
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    checkNotClosed();
    try {
      return physicalConnection.getClientInfo();
    } catch (SQLException sqlException) {
      pooledCassandraConnection.connectionErrorOccurred(sqlException);
      throw sqlException;
    }
  }

  @Override
  public void setClientInfo(Properties properties) {
    if (!isClosed()) {
      physicalConnection.setClientInfo(properties);
    }
  }

  @Override
  public String getSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setSchema(String arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public Map<String, Class<?>> getTypeMap() {
    // TODO Auto-generated method stub
    return null;
  }
}
