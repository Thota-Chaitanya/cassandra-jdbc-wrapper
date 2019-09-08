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

import static com.github.adejanovski.cassandra.jdbc.result.set.CassandraResultSet.DEFAULT_CONCURRENCY;
import static com.github.adejanovski.cassandra.jdbc.result.set.CassandraResultSet.DEFAULT_HOLDABILITY;
import static com.github.adejanovski.cassandra.jdbc.result.set.CassandraResultSet.DEFAULT_TYPE;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.ALWAYS_AUTOCOMMIT;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.BAD_TIMEOUT;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NO_INTERFACE;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NO_TRANSACTIONS;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.PROTOCOL;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_ACTIVE_CQL_VERSION;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_CONSISTENCY_LEVEL;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_CQL_VERSION;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_DATABASE_NAME;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_DEBUG;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_USER;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.WAS_CLOSED_CON;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.createSubName;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UserType;
import com.github.adejanovski.cassandra.jdbc.meta.data.CassandraDatabaseMetaData;
import com.github.adejanovski.cassandra.jdbc.session.SessionHolder;
import com.github.adejanovski.cassandra.jdbc.statement.CassandraPreparedStatement;
import com.github.adejanovski.cassandra.jdbc.statement.CassandraStatement;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation class for {@link Connection}.
 */
public class CassandraConnection extends AbstractConnection implements Connection {

  private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

  public static volatile int DB_MAJOR_VERSION = 1;
  public static volatile int DB_MINOR_VERSION = 2;
  public static volatile int DB_REVISION = 2;
  public static final String DB_PRODUCT_NAME = "Cassandra";
  private static final String DEFAULT_CQL_VERSION = "3.0.0";
  private ConcurrentMap<String, CassandraPreparedStatement> preparedStatements = Maps.newConcurrentMap();

  private final SessionHolder sessionHolder;

  /**
   * Connection Properties
   */
  private Properties connectionProps;

  /**
   * Client Info Properties (currently unused)
   */
  private Properties clientInfo = new Properties();

  /**
   * Set of all Statements that have been created by this connection
   */
  private Set<Statement> statements = new ConcurrentSkipListSet<>();

  private Session cSession;

  public String username;
  public String url;
  private String currentKeyspace;
  private int majorCqlVersion;
  private Metadata metadata;
  public boolean debugMode;
  private volatile boolean isClosed;


  PreparedStatement isAlive = null;

  //private String currentCqlVersion;

  public ConsistencyLevel defaultConsistencyLevel;

  /**
   * Instantiates a new CassandraConnection.
   */
  public CassandraConnection(SessionHolder sessionHolder) throws SQLException {
    this.sessionHolder = sessionHolder;
    Properties props = sessionHolder.properties;

    debugMode = props.getProperty(TAG_DEBUG, "").equals("true");
    connectionProps = (Properties) props.clone();
    clientInfo = new Properties();
    url = PROTOCOL + createSubName(props);
    currentKeyspace = props.getProperty(TAG_DATABASE_NAME);
    username = props.getProperty(TAG_USER, "");
    String version = props.getProperty(TAG_CQL_VERSION, DEFAULT_CQL_VERSION);
    connectionProps.setProperty(TAG_ACTIVE_CQL_VERSION, version);
    majorCqlVersion = getMajor(version);
    defaultConsistencyLevel = ConsistencyLevel.valueOf(props.getProperty(TAG_CONSISTENCY_LEVEL, ConsistencyLevel.ONE.name()));

    cSession = sessionHolder.session;

    metadata = cSession.getCluster().getMetadata();
    logger.info("Connected to cluster: %s\n", metadata.getClusterName());
    for (Host aHost : metadata.getAllHosts()) {
      logger.info("Data center: %s; Host: %s; Rack: %s\n",
          aHost.getDatacenter(), aHost.getAddress(), aHost.getRack());
    }

    Iterator<Host> hosts = metadata.getAllHosts().iterator();
    if (hosts.hasNext()) {
      Host firstHost = hosts.next();
      // TODO this is shared among all Connections, what if they belong to different clusters?
      CassandraConnection.DB_MAJOR_VERSION = firstHost.getCassandraVersion().getMajor();
      CassandraConnection.DB_MINOR_VERSION = firstHost.getCassandraVersion().getMinor();
      CassandraConnection.DB_REVISION = firstHost.getCassandraVersion().getPatch();
    }
  }

  // get the Major portion of a string like : Major.minor.patch where 2 is the default
  @SuppressWarnings("boxing")
  private int getMajor(String version) {
    int major = 0;
    String[] parts = version.split("\\.");
    try {
      major = Integer.parseInt(parts[0]);
    } catch (Exception e) {
      major = 2;
    }
    return major;
  }

  private void checkNotClosed() throws SQLException {
      if (isClosed()) {
          throw new SQLNonTransientConnectionException(WAS_CLOSED_CON);
      }
  }

  public void clearWarnings() throws SQLException {
    // This implementation does not support the collection of warnings so clearing is a no-op
    // but it is still an exception to call this on a closed connection.
    checkNotClosed();
  }

  /**
   * On close of connection.
   */
  public void close() {
    sessionHolder.release();
    isClosed = true;
  }

  public void commit() throws SQLException {
    checkNotClosed();
    //throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
  }

  public Statement createStatement() throws SQLException {
    checkNotClosed();
    Statement statement = new CassandraStatement(this);

    statements.add(statement);
    return statement;
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    checkNotClosed();
    Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency);
    statements.add(statement);
    return statement;
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    checkNotClosed();
    Statement statement = new CassandraStatement(this, null, resultSetType, resultSetConcurrency, resultSetHoldability);
    statements.add(statement);
    return statement;
  }

  public boolean getAutoCommit() throws SQLException {
    checkNotClosed();
    return true;
  }

  public String getCatalog() throws SQLException {
    checkNotClosed();
    return metadata.getClusterName();
  }

  public void setSchema(String schema) throws SQLException {
    checkNotClosed();
    currentKeyspace = schema;
  }

  public String getSchema() throws SQLException {
    checkNotClosed();
    return currentKeyspace;
  }

  public Properties getClientInfo() throws SQLException {
    checkNotClosed();
    return clientInfo;
  }

  public String getClientInfo(String label) throws SQLException {
    checkNotClosed();
    return clientInfo.getProperty(label);
  }

  public int getHoldability() throws SQLException {
    checkNotClosed();
    // the rationale is there are really no commits in Cassandra so no boundary...
    return DEFAULT_HOLDABILITY;
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    checkNotClosed();
    return new CassandraDatabaseMetaData(this);
  }

  public int getTransactionIsolation() throws SQLException {
    checkNotClosed();
    return Connection.TRANSACTION_NONE;
  }

  public SQLWarning getWarnings() throws SQLException {
    checkNotClosed();
    // the rationale is there are no warnings to return in this implementation...
    return null;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public boolean isReadOnly() throws SQLException {
    checkNotClosed();
    return false;
  }

  public boolean isValid(int timeout) throws SQLTimeoutException {
      if (timeout < 0) {
          throw new SQLTimeoutException(BAD_TIMEOUT);
      }

    // set timeout
/*
        try
        {
        	if (isClosed()) {
        		return false;
        	}
        	
            if (isAlive == null)
            {
                isAlive = prepareStatement(currentCqlVersion == "2.0.0" ? IS_VALID_CQLQUERY_2_0_0 : IS_VALID_CQLQUERY_3_0_0);
            }
            // the result is not important
            isAlive.executeQuery().close();
        }
        catch (SQLException e)
        {
        	return false;
        }
        finally {
            // reset timeout
            socket.setTimeout(0);
        }
*/
    return true;
  }

  public boolean isWrapperFor(Class<?> arg0) {
    return false;
  }

  public String nativeSQL(String sql) throws SQLException {
    checkNotClosed();
    // the rationale is there are no distinction between grammars in this implementation...
    // so we are just return the input argument
    return sql;
  }

  public CassandraPreparedStatement prepareStatement(String cql) throws SQLException {
    CassandraPreparedStatement prepStmt = preparedStatements.get(cql);
    if (prepStmt == null) {
      // Statement didn't exist
      prepStmt = preparedStatements.putIfAbsent(cql, prepareStatement(cql, DEFAULT_TYPE, DEFAULT_CONCURRENCY, DEFAULT_HOLDABILITY));
      if (prepStmt == null) {
        // Statement has already been created by another thread, so we'll just get it
        return preparedStatements.get(cql);
      }
    }

    return prepStmt;
  }

  public CassandraPreparedStatement prepareStatement(String cql, int rsType) throws SQLException {
    return prepareStatement(cql, rsType, DEFAULT_CONCURRENCY, DEFAULT_HOLDABILITY);
  }

  public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency) throws SQLException {
    return prepareStatement(cql, rsType, rsConcurrency, DEFAULT_HOLDABILITY);
  }

  public CassandraPreparedStatement prepareStatement(String cql, int rsType, int rsConcurrency, int rsHoldability) throws SQLException {
    checkNotClosed();
    CassandraPreparedStatement statement = new CassandraPreparedStatement(this, cql, rsType, rsConcurrency, rsHoldability);
    statements.add(statement);
    return statement;
  }

  public void rollback() throws SQLException {
    checkNotClosed();
    throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    checkNotClosed();
    //if (!autoCommit) throw new SQLFeatureNotSupportedException(ALWAYS_AUTOCOMMIT);
  }

  public void setCatalog(String arg0) throws SQLException {
    checkNotClosed();
    // the rationale is there are no catalog name to set in this implementation...
    // so we are "silently ignoring" the request
  }

  public void setClientInfo(Properties props) {
    // we don't use them but we will happily collect them for now...
      if (props != null) {
          clientInfo = props;
      }
  }

  public void setClientInfo(String key, String value) {
    // we don't use them but we will happily collect them for now...
    clientInfo.setProperty(key, value);
  }

  public void setHoldability(int arg0) throws SQLException {
    checkNotClosed();
    // the rationale is there are no holdability to set in this implementation...
    // so we are "silently ignoring" the request
  }

  public void setReadOnly(boolean arg0) throws SQLException {
    checkNotClosed();
    // the rationale is all connections are read/write in the Cassandra implementation...
    // so we are "silently ignoring" the request
  }

  public void setTransactionIsolation(int level) throws SQLException {
    checkNotClosed();
      if (level != Connection.TRANSACTION_NONE) {
          throw new SQLFeatureNotSupportedException(NO_TRANSACTIONS);
      }
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
  }

  /**
   * Remove a Statement from the Open Statements List
   */
  public void removeStatement(Statement statement) {
    statements.remove(statement);
  }

  public String toString() {
    return "CassandraConnection [connectionProps="
        + connectionProps
        + "]";
  }


  public Session getSession() {
    return this.cSession;
  }

  public Metadata getClusterMetadata() {
    return metadata;
  }

  public Map<String, Class<?>> getTypeMap() {
    HashMap<String, Class<?>> typeMap = new HashMap<>();
    logger.info("current KS : " + currentKeyspace);
    Collection<UserType> types = this.metadata.getKeyspace(currentKeyspace).getUserTypes();
    for (UserType type : types) {
      typeMap.put(type.getTypeName(), type.getClass());
    }

    return typeMap;
  }


  public Properties getConnectionProps() {
    return connectionProps;
  }
}
