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

package com.github.adejanovski.cassandra.jdbc.data.source;

import static com.github.adejanovski.cassandra.jdbc.utils.Utils.HOST_REQUIRED;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NOT_SUPPORTED;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NO_INTERFACE;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.PROTOCOL;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_CONSISTENCY_LEVEL;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_CQL_VERSION;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_DATABASE_NAME;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_PASSWORD;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_PORT_NUMBER;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_SERVER_NAME;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.TAG_USER;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.createSubName;

import com.github.adejanovski.cassandra.jdbc.connection.CassandraConnection;
import com.github.adejanovski.cassandra.jdbc.connection.PooledCassandraConnection;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;

public class CassandraDataSource implements ConnectionPoolDataSource, DataSource {

  static {
    try {
      Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static final String description = "Cassandra Data Source";

  private String serverName;

  private int portNumber = 9042;

  private String databaseName;

  private String user;

  private String password;

  protected String version = null;

  private String consistency = null;

  public CassandraDataSource(String host, int port, String keyspace, String user, String password, String version, String consistency) {
      if (host != null) {
          setServerName(host);
      }
      if (port != -1) {
          setPortNumber(port);
      }
      if (version != null) {
          setVersion(version);
      }
      if (consistency != null) {
          setConsistency(consistency);
      }
    setDatabaseName(keyspace);
    setUser(user);
    setPassword(password);
  }

  public String getDescription() {
    return description;
  }

  public String getServerName() {
    return serverName;
  }

  private void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getConsistency() {
    return consistency;
  }

  private void setConsistency(String consistency) {
    this.consistency = consistency;
  }

  public int getPortNumber() {
    return portNumber;
  }

  private void setPortNumber(int portNumber) {
    this.portNumber = portNumber;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  private void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getUser() {
    return user;
  }

  private void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  private void setPassword(String password) {
    this.password = password;
  }

  public CassandraConnection getConnection() throws SQLException {
    return getConnection(null, null);
  }

  public CassandraConnection getConnection(String user, String password) throws SQLException {
    Properties props = new Properties();

    this.user = user;
    this.password = password;

      if (this.serverName != null) {
          props.setProperty(TAG_SERVER_NAME, this.serverName);
      } else {
          throw new SQLNonTransientConnectionException(HOST_REQUIRED);
      }
    props.setProperty(TAG_PORT_NUMBER, "" + this.portNumber);
      if (this.databaseName != null) {
          props.setProperty(TAG_DATABASE_NAME, this.databaseName);
      }
      if (user != null) {
          props.setProperty(TAG_USER, user);
      }
      if (password != null) {
          props.setProperty(TAG_PASSWORD, password);
      }
      if (this.version != null) {
          props.setProperty(TAG_CQL_VERSION, version);
      }
      if (this.consistency != null) {
          props.setProperty(TAG_CONSISTENCY_LEVEL, consistency);
      }

    String url = PROTOCOL + createSubName(props);
    return (CassandraConnection) DriverManager.getConnection(url, props);
  }

  public int getLoginTimeout() {
    return DriverManager.getLoginTimeout();
  }

  public PrintWriter getLogWriter() {
    return DriverManager.getLogWriter();
  }

  public void setLoginTimeout(int timeout) {
    DriverManager.setLoginTimeout(timeout);
  }

  public void setLogWriter(PrintWriter writer) {
    DriverManager.setLogWriter(writer);
  }

  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(getClass());
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isAssignableFrom(getClass())) {
          return iface.cast(this);
      }
    throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  @Override
  public PooledCassandraConnection getPooledConnection() throws SQLException {
    return new PooledCassandraConnection(getConnection());
  }

  @Override
  public PooledCassandraConnection getPooledConnection(String user, String password) throws SQLException {
    return new PooledCassandraConnection(getConnection(user, password));
  }
}
