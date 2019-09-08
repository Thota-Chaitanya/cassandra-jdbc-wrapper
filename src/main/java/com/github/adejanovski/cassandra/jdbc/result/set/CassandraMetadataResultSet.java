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

package com.github.adejanovski.cassandra.jdbc.result.set;

import static com.github.adejanovski.cassandra.jdbc.utils.Utils.BAD_FETCH_DIR;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.BAD_FETCH_SIZE;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.FORWARD_ONLY;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.MUST_BE_POSITIVE;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.NO_INTERFACE;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.VALID_LABELS;
import static com.github.adejanovski.cassandra.jdbc.utils.Utils.WAS_CLOSED_RSLT;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.github.adejanovski.cassandra.jdbc.meta.data.MetadataRow;
import com.github.adejanovski.cassandra.jdbc.statement.CassandraStatement;
import com.github.adejanovski.cassandra.jdbc.type.AbstractJdbcType;
import com.github.adejanovski.cassandra.jdbc.type.JdbcAscii;
import com.github.adejanovski.cassandra.jdbc.type.JdbcBytes;
import com.github.adejanovski.cassandra.jdbc.type.JdbcInt32;
import com.github.adejanovski.cassandra.jdbc.type.JdbcLong;
import com.github.adejanovski.cassandra.jdbc.type.JdbcShort;
import com.github.adejanovski.cassandra.jdbc.type.JdbcUTF8;
import com.github.adejanovski.cassandra.jdbc.type.JdbcUUID;
import com.github.adejanovski.cassandra.jdbc.type.TypesMap;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The Supported Data types in CQL are as follows:
 * </p>
 * <table>
 * <tr>
 * <th>type</th>
 * <th>java type</th>
 * <th>description</th>
 * </tr>
 * <tr>
 * <td>ascii</td>
 * <td>String</td>
 * <td>ASCII character string</td>
 * </tr>
 * <tr>
 * <td>bigint</td>
 * <td>Long</td>
 * <td>64-bit signed long</td>
 * </tr>
 * <tr>
 * <td>blob</td>
 * <td>ByteBuffer</td>
 * <td>Arbitrary bytes (no validation)</td>
 * </tr>
 * <tr>
 * <td>boolean</td>
 * <td>Boolean</td>
 * <td>true or false</td>
 * </tr>
 * <tr>
 * <td>counter</td>
 * <td>Long</td>
 * <td>Counter column (64-bit long)</td>
 * </tr>
 * <tr>
 * <td>decimal</td>
 * <td>BigDecimal</td>
 * <td>Variable-precision decimal</td>
 * </tr>
 * <tr>
 * <td>double</td>
 * <td>Double</td>
 * <td>64-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>float</td>
 * <td>Float</td>
 * <td>32-bit IEEE-754 floating point</td>
 * </tr>
 * <tr>
 * <td>int</td>
 * <td>Integer</td>
 * <td>32-bit signed int</td>
 * </tr>
 * <tr>
 * <td>text</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>timestamp</td>
 * <td>Date</td>
 * <td>A timestamp</td>
 * </tr>
 * <tr>
 * <td>uuid</td>
 * <td>UUID</td>
 * <td>Type 1 or type 4 UUID</td>
 * </tr>
 * <tr>
 * <td>varchar</td>
 * <td>String</td>
 * <td>UTF8 encoded string</td>
 * </tr>
 * <tr>
 * <td>varint</td>
 * <td>BigInteger</td>
 * <td>Arbitrary-precision integer</td>
 * </tr>
 * </table>
 */
class CassandraMetadataResultSet extends AbstractResultSet implements CassandraResultSetExtras {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(CassandraMetadataResultSet.class);

  private MetadataRow currentRow;

  //private ColumnDefinitions colDefinitions;
  //private com.datastax.driver.core.ResultSet datastaxRs;
  /**
   * The rows iterator.
   */
  private Iterator<MetadataRow> rowsIterator;


  private int rowNumber = 0;
  // the current row key when iterating through results.
  private byte[] curRowKey = null;

  /**
   * The index map.
   */
  // private Map<String, Integer> indexMap = new HashMap<String, Integer>();

  private final CResultSetMetaData meta;

  private final CassandraStatement statement;

  private int resultSetType;

  private int fetchDirection;

  private int fetchSize;

  private boolean wasNull;

  private MetadataResultSet driverResultSet;

  //private CqlMetadata schema;

  /**
   * Instantiates a new cassandra result set from a com.datastax.driver.core.ResultSet.
   */
  CassandraMetadataResultSet(CassandraStatement statement, MetadataResultSet metadataResultSet) throws SQLException {
    this.statement = statement;
    this.resultSetType = statement.getResultSetType();
    this.fetchDirection = statement.getFetchDirection();
    this.fetchSize = statement.getFetchSize();
    this.driverResultSet = metadataResultSet;
    //this.schema = resultSet.schema;

    // Initialize meta-data from schema
    populateMetaData();

    rowsIterator = metadataResultSet.iterator();
    //colDefinitions = metadataResultSet.getColumnDefinitions();

    // Initialize to column values from the first row
    // re-Initialize meta-data to column values from the first row (if data exists)
    // NOTE: that the first call to next() will HARMLESSLY re-write these values for the columns
    // NOTE: the row cursor is not advanced and sits before the first row
    if (hasMoreRows()) {
      populateColumns();
      // reset the iterator back to the beginning.
      //rowsIterator = resultSet.iterator();
    }

    meta = new CResultSetMetaData();
  }


  private boolean hasMoreRows() {
    return (rowsIterator != null && (rowsIterator.hasNext() || (rowNumber == 0 && currentRow != null)));
  }

  private void populateMetaData() {
    // not used anymore

  }

  private void populateColumns() {
    currentRow = rowsIterator.next();

  }

  public boolean absolute(int arg0) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public void afterLast() throws SQLNonTransientException {
    if (resultSetType == TYPE_FORWARD_ONLY) {
      throw new SQLNonTransientException(FORWARD_ONLY);
    }
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public void beforeFirst() throws SQLNonTransientException {
    if (resultSetType == TYPE_FORWARD_ONLY) {
      throw new SQLNonTransientException(FORWARD_ONLY);
    }
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  private void checkIndex(int index) throws SQLSyntaxErrorException {

    if (currentRow != null) {
      wasNull = currentRow.isNull(index - 1);
      if (currentRow.getColumnDefinitions() != null) {
        if (index < 1 || index > currentRow.getColumnDefinitions().asList().size()) {
          throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, String.valueOf(index)) + " " + currentRow.getColumnDefinitions().asList().size());
        }
      }
    } else if (driverResultSet != null) {
      if (driverResultSet.getColumnDefinitions() != null) {
        if (index < 1 || index > driverResultSet.getColumnDefinitions().asList().size()) {
          throw new SQLSyntaxErrorException(String.format(MUST_BE_POSITIVE, String.valueOf(index)) + " " + driverResultSet.getColumnDefinitions().asList().size());
        }
      }
    }

  }

  private void checkName(String name) throws SQLSyntaxErrorException {

    if (currentRow != null) {
      wasNull = currentRow.isNull(name);
      if (!currentRow.getColumnDefinitions().contains(name)) {
        throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
      }
    } else if (driverResultSet != null) {
      if (driverResultSet.getColumnDefinitions() != null) {
        if (!driverResultSet.getColumnDefinitions().contains(name)) {
          throw new SQLSyntaxErrorException(String.format(VALID_LABELS, name));
        }
      }

    }
  }

  private void checkNotClosed() throws SQLRecoverableException {
    if (isClosed()) {
      throw new SQLRecoverableException(WAS_CLOSED_RSLT);
    }
  }

  public void clearWarnings() throws SQLRecoverableException {
    // This implementation does not support the collection of warnings so clearing is a no-op
    // but it is still an exception to call this on a closed connection.
    checkNotClosed();
  }

  public void close()  {
        /* indexMap = null;
        values = null; */
    if (!isClosed()) {
      this.statement.close();
    }
  }

  public int findColumn(String name) throws SQLRecoverableException, SQLSyntaxErrorException {
    checkNotClosed();
    checkName(name);
    return currentRow.getColumnDefinitions().getIndexOf(name);
  }

  public boolean first() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }


  public BigDecimal getBigDecimal(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    return currentRow.getDecimal(index - 1);
  }

  /**
   * @deprecated
   */
  public BigDecimal getBigDecimal(int index, int scale) throws SQLSyntaxErrorException {
    checkIndex(index);
    return currentRow.getDecimal(index - 1).setScale(scale);
  }

  public BigDecimal getBigDecimal(String name) throws SQLSyntaxErrorException {
    checkName(name);
    return currentRow.getDecimal(name);
  }

  /**
   * @deprecated
   */
  public BigDecimal getBigDecimal(String name, int scale) throws SQLSyntaxErrorException {
    checkName(name);
    return currentRow.getDecimal(name).setScale(scale);
  }


  public BigInteger getBigInteger(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    return currentRow.getVarint(index - 1);
  }

  public BigInteger getBigInteger(String name) throws SQLSyntaxErrorException {
    checkName(name);
    return currentRow.getVarint(name);
  }


  public boolean getBoolean(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    return currentRow.getBool(index - 1);
  }

  public boolean getBoolean(String name) throws SQLSyntaxErrorException {
    checkName(name);
    return currentRow.getBool(name);
  }

  public byte getByte(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    return currentRow.getBytes(index - 1).get();
  }

  public byte getByte(String name) throws SQLSyntaxErrorException {
    checkName(name);
    return currentRow.getBytes(name).get();
  }


  public byte[] getBytes(int index)  {
    return currentRow.getBytes(index - 1).array();
  }

  public byte[] getBytes(String name)  {
    return currentRow.getBytes(name).array();
  }


  public int getConcurrency() throws SQLException {
    checkNotClosed();
    return statement.getResultSetConcurrency();
  }

  public Date getDate(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    if (currentRow.getDate(index - 1) == null) {
      return null;
    }
    return new java.sql.Date(currentRow.getDate(index - 1).getTime());
  }

  public Date getDate(int index, Calendar calendar) throws SQLSyntaxErrorException {
    checkIndex(index);
    // silently ignore the Calendar argument; its a hint we do not need
    return getDate(index - 1);
  }

  public Date getDate(String name) throws SQLSyntaxErrorException {
    checkName(name);
    if (currentRow.getDate(name) == null) {
      return null;
    }
    return new java.sql.Date(currentRow.getDate(name).getTime());
  }

  public Date getDate(String name, Calendar calendar) throws SQLSyntaxErrorException {
    checkName(name);
    // silently ignore the Calendar argument; its a hint we do not need
    return getDate(name);
  }


  public double getDouble(int index) throws SQLNonTransientException {
    checkIndex(index);

    try {
      if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("float")) {
        return currentRow.getFloat(index - 1);
      }
      return currentRow.getDouble(index - 1);
    } catch (InvalidTypeException e) {
      throw new SQLNonTransientException(e);
    }
  }

  public double getDouble(String name) throws SQLNonTransientException {
    checkName(name);
    try {
      if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("float")) {
        return currentRow.getFloat(name);
      }
      return currentRow.getDouble(name);
    } catch (InvalidTypeException e) {
      throw new SQLNonTransientException(e);
    }
  }


  public int getFetchDirection() throws SQLRecoverableException {
    checkNotClosed();
    return fetchDirection;
  }

  public int getFetchSize() throws SQLRecoverableException {
    checkNotClosed();
    return fetchSize;
  }

  public float getFloat(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    return currentRow.getFloat(index - 1);
  }

  public float getFloat(String name) throws SQLSyntaxErrorException {
    checkName(name);
    return currentRow.getFloat(name);
  }


  public int getHoldability() throws SQLException {
    checkNotClosed();
    return statement.getResultSetHoldability();
  }

  public int getInt(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    return currentRow.getInt(index - 1);

  }

  public int getInt(String name) throws SQLSyntaxErrorException {
    checkName(name);

    return currentRow.getInt(name);
  }


  public byte[] getKey()  {
    return curRowKey;
  }

  public List<?> getList(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    if (currentRow.getColumnDefinitions().getType(index - 1).isCollection()) {
      try {
        return Lists.newArrayList(
            currentRow.getList(index - 1, Class.forName(currentRow.getColumnDefinitions().getType(index - 1).getTypeArguments().get(0).getClass().getCanonicalName())));
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();

      } // TODO: a remplacer par une vraie verification des types de collections
    }
    return currentRow.getList(index - 1, String.class);
  }

  public List<?> getList(String name) throws SQLSyntaxErrorException {
    checkName(name);
    // return currentRow.getList(name,String.class); // TODO: a remplacer par une vraie verification des types de collections
    if (currentRow.getColumnDefinitions().getType(name).isCollection()) {
      try {
        return Lists.newArrayList(currentRow.getList(name, Class.forName(currentRow.getColumnDefinitions().getType(name).getTypeArguments().get(0).getClass().getCanonicalName())));
      } catch (ClassNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();

      } // TODO: a remplacer par une vraie verification des types de collections
    }
    return currentRow.getList(name, String.class);
  }


  public long getLong(int index) throws SQLNonTransientException {
    checkIndex(index);
    try {
      if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("int")) {
        return currentRow.getInt(index - 1);
      } else if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("varint")) {
        return currentRow.getVarint(index - 1).longValue();
      } else {
        return currentRow.getLong(index - 1);
      }
    } catch (InvalidTypeException e) {
      throw new SQLNonTransientException(e);
    }

    //return currentRow.getLong(index - 1);

  }

  public long getLong(String name) throws SQLNonTransientException {
    checkName(name);
    try {
      if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("int")) {
        return currentRow.getInt(name);
      } else if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("varint")) {
        return currentRow.getVarint(name).longValue();
      } else {
        return currentRow.getLong(name);
      }
    } catch (InvalidTypeException e) {
      throw new SQLNonTransientException(e);
    }
  }


  public Map<?, ?> getMap(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    wasNull = currentRow.isNull(index - 1);
    return currentRow.getMap(index - 1, String.class, String.class); // TODO: a remplacer par une vraie verification des types de collections
  }

  public Map<?, ?> getMap(String name) throws SQLSyntaxErrorException {
    checkName(name);

    return currentRow.getMap(name, String.class, String.class); // TODO: a remplacer par une vraie verification des types de collections
  }


  public ResultSetMetaData getMetaData() throws SQLRecoverableException {
    checkNotClosed();
    return meta;
  }

  @SuppressWarnings("boxing")
  public Object getObject(int index) throws SQLSyntaxErrorException {
    checkIndex(index);

    String typeName = currentRow.getColumnDefinitions().getType(index - 1).getName().toString();
    switch (typeName) {
      case "varchar":
      case "ascii":
      case "text":
        return currentRow.getString(index - 1);
      case "integer":
      case "int":
      case "varint":
        return currentRow.getInt(index - 1);
      case "bigint":
      case "counter":
        return currentRow.getLong(index - 1);
      case "blob":
        return currentRow.getBytes(index - 1);
      case "boolean":
        return currentRow.getBool(index - 1);
      case "decimal":
        return currentRow.getDecimal(index - 1);
      case "double":
        return currentRow.getDouble(index - 1);
      case "float":
        return currentRow.getFloat(index - 1);
      case "inet":
        return currentRow.getInet(index - 1);
      case "timestamp":
        return new Timestamp((currentRow.getDate(index - 1)).getTime());
      case "uuid":
      case "timeuuid":
        return currentRow.getUUID(index - 1);
      case "smallint":
        return currentRow.getShort(index - 1);
      case "time":
        return getTime(index);
    }

    return null;
  }

  @SuppressWarnings("boxing")
  public Object getObject(String name) throws SQLSyntaxErrorException {
    checkName(name);

    String typeName = currentRow.getColumnDefinitions().getType(name).getName().toString();

    switch (typeName) {
      case "varchar":
      case "ascii":
      case "text":
        return currentRow.getString(name);
      case "integer":
      case "int":
      case "varint":
        return currentRow.getInt(name);
      case "bigint":
      case "counter":
        return currentRow.getLong(name);
      case "blob":
        return currentRow.getBytes(name);
      case "boolean":
        return currentRow.getBool(name);
      case "decimal":
        return currentRow.getDecimal(name);
      case "double":
        return currentRow.getDouble(name);
      case "float":
        return currentRow.getFloat(name);
      case "inet":
        return currentRow.getInet(name);
      case "timestamp":
        return new Timestamp((currentRow.getDate(name)).getTime());
      case "uuid":
      case "timeuuid":
        return currentRow.getUUID(name);
      case "smallint":
        return currentRow.getShort(name);
      case "time":
        return getTime(name);
    }

    return null;
  }


  public int getRow() throws SQLRecoverableException {
    checkNotClosed();
    return rowNumber;
  }


  public short getShort(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    return (short) currentRow.getInt(index - 1);
  }

  public Set<?> getSet(int index) throws SQLNonTransientException {
    checkIndex(index);
    try {
      return currentRow.getSet(index - 1, Class.forName(currentRow.getColumnDefinitions().getType(index - 1).getTypeArguments().get(0).getClass().getCanonicalName()));
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      throw new SQLNonTransientException(e);
    }

  }

  public Set<?> getSet(String name) throws SQLNonTransientException {
    checkName(name);
    try {
      return currentRow.getSet(name, Class.forName(currentRow.getColumnDefinitions().getType(name).getTypeArguments().get(0).getClass().getCanonicalName()));
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      throw new SQLNonTransientException(e);
    }

  }


  public short getShort(String name) throws SQLSyntaxErrorException {
    checkName(name);
    return (short) currentRow.getInt(name);
  }


  public Statement getStatement() throws SQLRecoverableException {
    checkNotClosed();
    return statement;
  }

  public String getString(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    try {
      if (currentRow.getColumnDefinitions().getType(index - 1).isCollection()) {
        return getObject(index).toString();
      }
      return currentRow.getString(index - 1);
    } catch (Exception e) {
      return getObject(index).toString();
    }
  }

  public String getString(String name) throws SQLSyntaxErrorException {
    checkName(name);
    try {
      if (currentRow.getColumnDefinitions().getType(name).isCollection()) {
        return getObject(name).toString();
      }
      return currentRow.getString(name);
    } catch (Exception e) {
      return getObject(name).toString();
    }
  }


  public Time getTime(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    LocalTime localTime = LocalTime.ofNanoOfDay(currentRow.getLong(index - 1));
    return Time.valueOf(localTime);
  }

  public Time getTime(int index, Calendar calendar) throws SQLSyntaxErrorException {
    checkIndex(index);
    // silently ignore the Calendar argument; its a hint we do not need
    return getTime(index);
  }

  public Time getTime(String name) throws SQLSyntaxErrorException {
    checkName(name);
    LocalTime localTime = LocalTime.ofNanoOfDay(currentRow.getLong(name));
    return Time.valueOf(localTime);
  }

  public Time getTime(String name, Calendar calendar) throws SQLSyntaxErrorException {
    checkName(name);
    // silently ignore the Calendar argument; its a hint we do not need
    return getTime(name);
  }

  public Timestamp getTimestamp(int index) throws SQLSyntaxErrorException {
    checkIndex(index);
    java.util.Date date = currentRow.getDate(index - 1);
    if (date == null) {
      return null;
    }
    return new Timestamp(currentRow.getDate(index - 1).getTime());
  }

  public Timestamp getTimestamp(int index, Calendar calendar) throws SQLSyntaxErrorException {
    checkIndex(index);
    // silently ignore the Calendar argument; its a hint we do not need
    java.util.Date date = currentRow.getDate(index - 1);
    if (date == null) {
      return null;
    }
    return getTimestamp(index - 1);
  }

  public Timestamp getTimestamp(String name) throws SQLSyntaxErrorException {
    checkName(name);
    java.util.Date date = currentRow.getDate(name);
    if (date == null) {
      return null;
    }
    return new Timestamp(currentRow.getDate(name).getTime());
  }

  public Timestamp getTimestamp(String name, Calendar calendar) throws SQLSyntaxErrorException {
    checkName(name);
    // silently ignore the Calendar argument; its a hint we do not need
    return getTimestamp(name);
  }


  public int getType() throws SQLRecoverableException {
    checkNotClosed();
    return resultSetType;
  }

  // URL (awaiting some clarifications as to how it is stored in C* ... just a validated Sting in URL format?
  public URL getURL(int arg0) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public URL getURL(String arg0) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  // These Methods are planned to be implemented soon; but not right now...
  // Each set of methods has a more detailed set of issues that should be considered fully...


  public SQLWarning getWarnings() throws SQLRecoverableException {
    checkNotClosed();
    // the rationale is there are no warnings to return in this implementation...
    return null;
  }


  public boolean isAfterLast() throws SQLRecoverableException {
    checkNotClosed();
    return rowNumber == Integer.MAX_VALUE;
  }

  public boolean isBeforeFirst() throws SQLRecoverableException {
    checkNotClosed();
    return rowNumber == 0;
  }

  public boolean isClosed()  {
    if (this.statement == null) {
      return true;
    }
    return this.statement.isClosed();
  }

  public boolean isFirst() throws SQLRecoverableException {
    checkNotClosed();
    return rowNumber == 1;
  }

  public boolean isLast() throws SQLRecoverableException {
    checkNotClosed();
    return !rowsIterator.hasNext();
  }

  public boolean isWrapperFor(Class<?> iface)  {
    return CassandraResultSetExtras.class.isAssignableFrom(iface);
  }

  // Navigation between rows within the returned set of rows
  // Need to use a list iterator so next() needs completely re-thought

  public boolean last() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public synchronized boolean next()  {
    if (hasMoreRows()) {
      // populateColumns is called upon init to set up the metadata fields; so skip first call
      if (rowNumber != 0) {
        populateColumns();
      }
      //else rowsIterator.next();
// populateColumns();
      rowNumber++;
      return true;
    }
    rowNumber = Integer.MAX_VALUE;
    return false;
  }


  public boolean previous() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public boolean relative(int arg0) throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  @SuppressWarnings("boxing")
  public void setFetchDirection(int direction) throws SQLSyntaxErrorException, SQLRecoverableException {
    checkNotClosed();

    if (direction == FETCH_FORWARD || direction == FETCH_REVERSE || direction == FETCH_UNKNOWN) {
      if ((getType() == TYPE_FORWARD_ONLY) && (direction != FETCH_FORWARD)) {
        throw new SQLSyntaxErrorException("attempt to set an illegal direction : " + direction);
      }
      fetchDirection = direction;
    }
    throw new SQLSyntaxErrorException(String.format(BAD_FETCH_DIR, direction));
  }

  @SuppressWarnings("boxing")
  public void setFetchSize(int size) throws SQLException {
    checkNotClosed();
    if (size < 0) {
      throw new SQLException(String.format(BAD_FETCH_SIZE, size));
    }
    fetchSize = size;
  }

  @SuppressWarnings("unchecked")
  public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException {
    if (iface.equals(CassandraResultSetExtras.class)) {
      return (T) this;
    }

    throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
  }

  public boolean wasNull()  {
    return wasNull;
  }


  /**
   * RSMD implementation. The metadata returned refers to the column values, not the column names.
   */
  class CResultSetMetaData implements ResultSetMetaData {

    /**
     * return the Cassandra Cluster Name as the Catalog
     */
    public String getCatalogName(int column) throws SQLException {
      //checkIndex(column);
      return statement.connection.getCatalog();
    }

    public String getColumnClassName(int column)  {/*
            checkIndex(column);
            return values.get(column - 1).getValueType().getType().getName();*/
      if (currentRow != null) {
        return currentRow.getColumnDefinitions().getType(column - 1).getName().toString();
      }
      return driverResultSet.getColumnDefinitions().asList().get(column - 1).getType().getName().toString();

    }

    public int getColumnCount()  {
      if (currentRow != null) {
        return currentRow.getColumnDefinitions().size();
      }
      return driverResultSet != null && driverResultSet.getColumnDefinitions() != null ? driverResultSet.getColumnDefinitions().size() : 0;
    }

    @SuppressWarnings("rawtypes")
    public int getColumnDisplaySize(int column)  {
      //checkIndex(column);
      try {
        AbstractJdbcType jtype;

        if (currentRow != null) {
          com.github.adejanovski.cassandra.jdbc.result.set.ColumnDefinitions.Definition col = currentRow.getColumnDefinitions().asList().get(column - 1);
          jtype = TypesMap.getTypeForComparator(col.getType().toString());
        } else {
          Definition col = driverResultSet.getColumnDefinitions().asList().get(column - 1);
          jtype = TypesMap.getTypeForComparator(col.getType().toString());
        }

        int length = -1;

        if (jtype instanceof JdbcBytes) {
          length = Integer.MAX_VALUE / 2;
        }
        if (jtype instanceof JdbcAscii || jtype instanceof JdbcUTF8) {
          length = Integer.MAX_VALUE;
        }
        if (jtype instanceof JdbcUUID) {
          length = 36;
        }
        if (jtype instanceof JdbcInt32) {
          length = 4;
        }
        if (jtype instanceof JdbcShort) {
          length = 2;
        }
        if (jtype instanceof JdbcLong) {
          length = 8;
        }
        // String stringValue = getObject(column).toString();
        //return (stringValue == null ? -1 : stringValue.length());

        return length;
      } catch (Exception e) {
        return -1;
      }
      //return -1;
    }

    public String getColumnLabel(int column)  {
      //checkIndex(column);
      return getColumnName(column);
    }

    public String getColumnName(int column)  {
      //checkIndex(column);

      try {
        if (currentRow != null) {
          return currentRow.getColumnDefinitions().getName(column - 1);
        }
        return driverResultSet.getColumnDefinitions().asList().get(column - 1).getName();
      } catch (Exception e) {
        return "";
      }
    }

    public int getColumnType(int column)  {
      DataType type;
      if (currentRow != null) {
        type = currentRow.getColumnDefinitions().getType(column - 1);
      } else {
        type = driverResultSet.getColumnDefinitions().asList().get(column - 1).getType();
      }
      return TypesMap.getTypeForComparator(type.toString()).getJdbcType();

    }

    /**
     * Spec says "database specific type name"; for Cassandra this means the AbstractType.
     */
    public String getColumnTypeName(int column)  {

      //checkIndex(column);
      DataType type;
      try {
        if (currentRow != null) {
          type = currentRow.getColumnDefinitions().getType(column - 1);
        } else {
          type = driverResultSet.getColumnDefinitions().getType(column - 1);
        }

        return type.toString();
      } catch (Exception e) {
        return DataType.varchar().toString();
      }
    }

    public int getPrecision(int column)  {
//            checkIndex(column);
//            TypedColumn col = values.get(column - 1);
//            return col.getValueType().getPrecision(col.getValue());
      return 0;
    }

    public int getScale(int column)  {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().getScale(tc.getValue());
      return 0;
    }

    /**
     * return the DEFAULT current Keyspace as the Schema Name
     */
    public String getSchemaName(int column) throws SQLException {
      //checkIndex(column);
      return statement.connection.getSchema();
    }

       /* public String getTableName(int column) 
        {
            throw new SQLFeatureNotSupportedException();
        }*/

    public boolean isAutoIncrement(int column)  {
      return true;
//            checkIndex(column);
//            return values.get(column - 1).getValueType() instanceof JdbcCounterColumn; // todo: check Value is correct.
    }

    public boolean isCaseSensitive(int column)  {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().isCaseSensitive();
      return true;
    }

    public boolean isCurrency(int column)  {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().isCurrency();
      return false;
    }

    public boolean isDefinitelyWritable(int column)  {
      //checkIndex(column);
      return isWritable(column);
    }

    /**
     * absence is the equivalent of null in Cassandra
     */
    public int isNullable(int column)  {
      //checkIndex(column);
      return ResultSetMetaData.columnNullable;
    }

    public boolean isReadOnly(int column)  {
      //checkIndex(column);
      return column == 0;
    }

    public boolean isSearchable(int column)  {
      //checkIndex(column);
      return false;
    }

    public boolean isSigned(int column)  {
//            checkIndex(column);
//            TypedColumn tc = values.get(column - 1);
//            return tc.getValueType().isSigned();
      return false;
    }

    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }

    public boolean isWritable(int column) {
      //checkIndex(column);
      return column > 0;
    }

    public <T> T unwrap(Class<T> iface) throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
    }

    @Override
    public String getTableName(int column) {
      String tableName;
      if (currentRow != null) {
        tableName = currentRow.getColumnDefinitions().getTable(column - 1);
      } else {
        tableName = driverResultSet.getColumnDefinitions().getTable(column - 1);
      }
      return tableName;
    }
  }


  @Override
  public RowId getRowId(int columnIndex) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RowId getRowId(String columnLabel) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLSyntaxErrorException {
    checkIndex(columnIndex);
    byte[] bytes = new byte[currentRow.getBytes(columnIndex - 1).remaining()];
    currentRow.getBytes(columnIndex - 1).get(bytes, 0, bytes.length);

    return new ByteArrayInputStream(bytes);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLSyntaxErrorException {
    // TODO Auto-generated method stub
    checkName(columnLabel);
    byte[] bytes = new byte[currentRow.getBytes(columnLabel).remaining()];
    currentRow.getBytes(columnLabel).get(bytes, 0, bytes.length);

    return new ByteArrayInputStream(bytes);
  }

  @Override
  public Blob getBlob(int index) throws SQLException {
    checkIndex(index);

    return new javax.sql.rowset.serial.SerialBlob(currentRow.getBytes(index - 1).array());
  }

  @Override
  public Blob getBlob(String columnName) throws SQLException {
    checkName(columnName);

    return new javax.sql.rowset.serial.SerialBlob(currentRow.getBytes(columnName).array());
  }


}
