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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.github.adejanovski.cassandra.jdbc.data.type.DataTypeEnum;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import lombok.extern.slf4j.Slf4j;

// @Slf4j
public class CassandraResultSet extends AbstractResultSet implements CassandraResultSetExtras {

  private static final Logger logger = LoggerFactory.getLogger(CassandraResultSet.class);

  public static final int DEFAULT_TYPE = ResultSet.TYPE_FORWARD_ONLY;
  public static final int DEFAULT_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
  public static final int DEFAULT_HOLDABILITY = ResultSet.HOLD_CURSORS_OVER_COMMIT;
  private Row currentRow;

  /**
   * The rows iterator.
   */
  private Iterator<Row> rowsIterator;

  private int rowNumber = 0;
  // the current row key when iterating through results.
  private byte[] curRowKey = null;

  private final CResultSetMetaData meta;

  private final CassandraStatement statement;

  private int resultSetType;

  private int fetchDirection;

  private int fetchSize;

  private boolean wasNull;

  private com.datastax.driver.core.ResultSet driverResultSet;
  //private ArrayList<com.datastax.driver.core.ResultSet> severalDriverResultSet;

  //private CqlMetadata schema;

  /**
   * no argument constructor.
   */
  public CassandraResultSet() {
    statement = null;
    meta = new CResultSetMetaData();
  }

  /**
   * Instantiates a new cassandra result set from a com.datastax.driver.core.ResultSet.
   */
  public CassandraResultSet(CassandraStatement statement, com.datastax.driver.core.ResultSet resultSet) throws SQLException {
    this.statement = statement;
    this.resultSetType = statement.getResultSetType();
    this.fetchDirection = statement.getFetchDirection();
    this.fetchSize = statement.getFetchSize();
    this.driverResultSet = resultSet;

    // Initialize meta-data from schema
    populateMetaData();

    //rowsIterators.add(resultSet.iterator());
    rowsIterator = resultSet.iterator();

    //colDefinitions = resultSet.getColumnDefinitions();

    if (hasMoreRows()) {
      populateColumns();
    }

    meta = new CResultSetMetaData();
  }

  /**
   * Instantiates a new cassandra result set from a com.datastax.driver.core.ResultSet.
   */
  public CassandraResultSet(CassandraStatement statement, ArrayList<com.datastax.driver.core.ResultSet> resultSets) throws SQLException {
    this.statement = statement;
    this.resultSetType = statement.getResultSetType();
    this.fetchDirection = statement.getFetchDirection();
    this.fetchSize = statement.getFetchSize();
    //this.rowsIterators = Lists.newArrayList();

    // We have several result sets, but we will use only the first one for metadata needs
    this.driverResultSet = resultSets.get(0);

    // Now we concatenate iterators of the different result sets into a single one and voilà !! ;)
    rowsIterator = driverResultSet.iterator();
    for (int i = 1; i < resultSets.size(); i++) {
      rowsIterator = Iterators.concat(rowsIterator, resultSets.get(i).iterator()); // this leads to Stack Overflow Exception when there are too many resultSets
        	/*if(resultSets.get(i).iterator().hasNext()){
        		rowsIterators.add(resultSets.get(i).iterator());
        	}
        	*/
    }

    //colDefinitions = driverResultSet.getColumnDefinitions();

    // Initialize to column values from the first row
    if (hasMoreRows()) {
      populateColumns();
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

  public boolean absolute(int arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public void afterLast() throws SQLException {
    if (resultSetType == TYPE_FORWARD_ONLY) {
      throw new SQLNonTransientException(FORWARD_ONLY);
    }
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public void beforeFirst() throws SQLException {
    if (resultSetType == TYPE_FORWARD_ONLY) {
      throw new SQLNonTransientException(FORWARD_ONLY);
    }
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  private void checkIndex(int index) throws SQLException {

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

  private void checkName(String name) throws SQLException {
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

  public void clearWarnings() throws SQLException {
    // This implementation does not support the collection of warnings so clearing is a no-op
    // but it is still an exception to call this on a closed connection.
    checkNotClosed();
  }

  public void close() {
    if (!isClosed()) {
      this.statement.close();
    }
  }

  public int findColumn(String name) throws SQLException {
    checkNotClosed();
    checkName(name);
    return currentRow.getColumnDefinitions().getIndexOf(name);
  }

  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }


  public BigDecimal getBigDecimal(int index) throws SQLException {
    checkIndex(index);
    return currentRow.getDecimal(index - 1);
  }

  /**
   * @deprecated
   */
  public BigDecimal getBigDecimal(int index, int scale) throws SQLException {
    checkIndex(index);
    return currentRow.getDecimal(index - 1).setScale(scale);
  }

  public BigDecimal getBigDecimal(String name) throws SQLException {
    checkName(name);
    return currentRow.getDecimal(name);
  }

  /**
   * @deprecated
   */
  public BigDecimal getBigDecimal(String name, int scale) throws SQLException {
    checkName(name);
    return currentRow.getDecimal(name).setScale(scale);
  }


  public boolean getBoolean(int index) throws SQLException {
    checkIndex(index);
    return currentRow.getBool(index - 1);
  }

  public boolean getBoolean(String name) throws SQLException {
    checkName(name);
    return currentRow.getBool(name);
  }

  public byte getByte(int index) throws SQLException {
    checkIndex(index);
    return currentRow.getBytes(index - 1).get();
  }

  public byte getByte(String name) throws SQLException {
    checkName(name);
    return currentRow.getBytes(name).get();
  }


  public byte[] getBytes(int index) {
    return currentRow.getBytes(index - 1).array();
  }

  public byte[] getBytes(String name) {
    return currentRow.getBytes(name).array();
  }

  public int getConcurrency() throws SQLException {
    checkNotClosed();
    return statement.getResultSetConcurrency();
  }

  public Date getDate(int index) throws SQLException {
    checkIndex(index);
    java.util.Date date = currentRow.get(index - 1, java.util.Date.class);
    if (date == null) {
      return null;
    }
    return new java.sql.Date(date.getTime());
  }

  public Date getDate(int index, Calendar calendar) throws SQLException {
    checkIndex(index);
    // silently ignore the Calendar argument; its a hint we do not need
    return getDate(index - 1);
  }

  public Date getDate(String name) throws SQLException {
    checkName(name);
    java.util.Date date = currentRow.get(name, java.util.Date.class);
    if (date == null) {
      return null;
    }
    return new java.sql.Date(date.getTime());
  }

  public Date getDate(String name, Calendar calendar) throws SQLException {
    checkName(name);
    // silently ignore the Calendar argument; its a hint we do not need
    return getDate(name);
  }


  public double getDouble(int index) throws SQLException {
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

  @SuppressWarnings("cast")
  public double getDouble(String name) throws SQLException {
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


  public int getFetchDirection() throws SQLException {
    checkNotClosed();
    return fetchDirection;
  }

  public int getFetchSize() throws SQLException {
    checkNotClosed();
    return fetchSize;
  }

  public float getFloat(int index) throws SQLException {
    checkIndex(index);
    return currentRow.getFloat(index - 1);
  }

  public float getFloat(String name) throws SQLException {
    checkName(name);
    return currentRow.getFloat(name);
  }


  public int getHoldability() throws SQLException {
    checkNotClosed();
    return statement.getResultSetHoldability();
  }

  public int getInt(int index) throws SQLException {
    checkIndex(index);
    return currentRow.getInt(index - 1);

  }

  public int getInt(String name) throws SQLException {
    checkName(name);

    return currentRow.getInt(name);
  }


  public byte[] getKey() {
    return curRowKey;
  }

  @SuppressWarnings("cast")
  public long getLong(int index) throws SQLException {
    checkIndex(index);
    try {
      switch (currentRow.getColumnDefinitions().getType(index - 1).getName().toString()) {
        case "int":
          return currentRow.getInt(index - 1);
        case "varint":
          return currentRow.getVarint(index - 1).longValue();
        case "smallint":
          return currentRow.getShort(index - 1);
        default:
          return currentRow.getLong(index - 1);
      }
    } catch (InvalidTypeException e) {
      throw new SQLNonTransientException(e);
    }

  }

  @SuppressWarnings("cast")
  public long getLong(String name) throws SQLException {
    checkName(name);
    try {
      switch (currentRow.getColumnDefinitions().getType(name).getName().toString()) {
        case "int":
          return currentRow.getInt(name);
        case "varint":
          return currentRow.getVarint(name).longValue();
        case "smallint":
          return currentRow.getShort(name);
        default:
          return currentRow.getLong(name);
      }
    } catch (InvalidTypeException e) {
      throw new SQLNonTransientException(e);
    }
  }

  public ResultSetMetaData getMetaData() {
    return meta;
  }

  @SuppressWarnings({"cast", "boxing"})
  public Object getObject(int index) throws SQLException {
    checkIndex(index);
    List<DataType> datatypes;

    if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("udt")) {
      return currentRow.getUDTValue(index - 1);
    }

    if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("tuple")) {
      return currentRow.getTupleValue(index - 1);
    }

    if (currentRow.getColumnDefinitions().getType(index - 1).isCollection()) {
      datatypes = currentRow.getColumnDefinitions().getType(index - 1).getTypeArguments();
      if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("set")) {
        if (datatypes.get(0).getName().toString().equals("udt")) {
          return Sets.newLinkedHashSet(currentRow.getSet(index - 1, TypesMap.getTypeForComparator("udt").getType()));
        } else if (datatypes.get(0).getName().toString().equals("tuple")) {
          if (datatypes.get(0).getName().toString().equals("udt")) {
            return Lists.newArrayList(currentRow.getList(index - 1, TypesMap.getTypeForComparator("udt").getType()));
          } else if (datatypes.get(0).getName().toString().equals("tuple")) {
            return Lists.newArrayList(currentRow.getList(index - 1, TypesMap.getTypeForComparator("tuple").getType()));
          } else {
            return Lists.newArrayList(currentRow.getList(index - 1, TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
          }

        } else {
          return Sets.newLinkedHashSet(currentRow.getSet(index - 1, TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
        }

      }
      if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("list")) {
        return Lists.newArrayList(currentRow.getList(index - 1, TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
      }
      if (currentRow.getColumnDefinitions().getType(index - 1).getName().toString().equals("map")) {

        Class<?> keyType = TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType();
        if (datatypes.get(0).getName().toString().equals("udt")) {
          keyType = TypesMap.getTypeForComparator("udt").getType();
        } else if (datatypes.get(0).getName().toString().equals("tuple")) {
          keyType = TypesMap.getTypeForComparator("tuple").getType();
        }

        Class<?> valueType = TypesMap.getTypeForComparator(datatypes.get(1).toString()).getType();
        if (datatypes.get(1).getName().toString().equals("udt")) {
          valueType = TypesMap.getTypeForComparator("udt").getType();
        } else if (datatypes.get(1).getName().toString().equals("tuple")) {
          valueType = TypesMap.getTypeForComparator("tuple").getType();
        }
        return Maps.newHashMap(currentRow.getMap(index - 1, keyType, valueType));
      }

    } else {
      String typeName = currentRow.getColumnDefinitions().getType(index - 1).getName().toString();
      switch (typeName) {
        case "varchar":
        case "text":
        case "ascii":
          return currentRow.getString(index - 1);
        case "integer":
        case "varint":
        case "int":
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
          java.util.Date date = currentRow.getTimestamp(index - 1);
          if (date == null) {
            return null;
          }
          return new Timestamp(date.getTime());
        case "uuid":
        case "timeuuid":
          return currentRow.getUUID(index - 1);
        case "time":
          return getTime(index);
        case "date":
          return getDate(index);
      }
    }

    return null;
  }

  @SuppressWarnings({"cast", "boxing"})
  public Object getObject(String name) throws SQLException {
    checkName(name);
    List<DataType> datatypes;

    if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("udt")) {
      return currentRow.getUDTValue(name);
    }

    if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("tuple")) {
      return currentRow.getTupleValue(name);
    }

    if (currentRow.getColumnDefinitions().getType(name).isCollection()) {
      datatypes = currentRow.getColumnDefinitions().getType(name).getTypeArguments();
      if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("set")) {
        if (datatypes.get(0).getName().toString().equals("udt")) {
          return Sets.newLinkedHashSet(currentRow.getSet(name, TypesMap.getTypeForComparator("udt").getType()));
        } else if (datatypes.get(0).getName().toString().equals("tuple")) {
          return Sets.newLinkedHashSet(currentRow.getSet(name, TypesMap.getTypeForComparator("tuple").getType()));
        } else {
          return Sets.newLinkedHashSet(currentRow.getSet(name, TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
        }
      }
      if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("list")) {
        if (datatypes.get(0).getName().toString().equals("udt")) {
          return Lists.newArrayList(currentRow.getList(name, TypesMap.getTypeForComparator("udt").getType()));
        } else if (datatypes.get(0).getName().toString().equals("tuple")) {
          return Lists.newArrayList(currentRow.getList(name, TypesMap.getTypeForComparator("tuple").getType()));
        } else {
          return Lists.newArrayList(currentRow.getList(name, TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType()));
        }
      }
      if (currentRow.getColumnDefinitions().getType(name).getName().toString().equals("map")) {
        Class<?> keyType = TypesMap.getTypeForComparator(datatypes.get(0).toString()).getType();
        if (datatypes.get(0).getName().toString().equals("udt")) {
          keyType = TypesMap.getTypeForComparator("udt").getType();
        } else if (datatypes.get(0).getName().toString().equals("tuple")) {
          keyType = TypesMap.getTypeForComparator("tuple").getType();

        }

        Class<?> valueType = TypesMap.getTypeForComparator(datatypes.get(1).toString()).getType();
        if (datatypes.get(1).getName().toString().equals("udt")) {
          valueType = TypesMap.getTypeForComparator("udt").getType();
        } else if (datatypes.get(1).getName().toString().equals("tuple")) {
          valueType = TypesMap.getTypeForComparator("tuple").getType();

        }

        return Maps.newHashMap(currentRow.getMap(name, keyType, valueType));
      }

    } else {
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
          return new Timestamp((currentRow.getTimestamp(name)).getTime());
        case "uuid":
        case "timeuuid":
          return currentRow.getUUID(name);
        case "time":
          return getTime(name);
        case "date":
          return getDate(name);
      }

    }

    return null;
  }


  public int getRow() throws SQLException {
    checkNotClosed();
    return rowNumber;
  }


  public short getShort(int index) throws SQLException {
    checkIndex(index);
    return currentRow.getShort(index - 1);
  }

  public short getShort(String name) throws SQLException {
    checkName(name);
    return currentRow.getShort(name);
  }


  public Statement getStatement() throws SQLException {
    checkNotClosed();
    return statement;
  }

  public String getString(int index) throws SQLException {
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

  public String getString(String name) throws SQLException {
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


  public Time getTime(int index) throws SQLException {
    checkIndex(index);
    LocalTime localTime = LocalTime.ofNanoOfDay(currentRow.getLong(index - 1));
    return Time.valueOf(localTime);
  }

  public Time getTime(int index, Calendar calendar) throws SQLException {
    checkIndex(index);
    // silently ignore the Calendar argument; its a hint we do not need
    return getTime(index);
  }

  public Time getTime(String name) throws SQLException {
    checkName(name);
    LocalTime localTime = LocalTime.ofNanoOfDay(currentRow.getLong(name));
    return Time.valueOf(localTime);
  }

  public Time getTime(String name, Calendar calendar) throws SQLException {
    checkName(name);
    // silently ignore the Calendar argument; its a hint we do not need
    return getTime(name);
  }

  public Timestamp getTimestamp(int index) throws SQLException {
    checkIndex(index);
    java.util.Date date = currentRow.getTimestamp(index - 1);
    if (date == null) {
      return null;
    }
    return new Timestamp(currentRow.getTimestamp(index - 1).getTime());
  }

  public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException {
    checkIndex(index);
    // silently ignore the Calendar argument; its a hint we do not need
    java.util.Date date = currentRow.getTimestamp(index - 1);
    if (date == null) {
      return null;
    }
    return getTimestamp(index - 1);
  }

  public Timestamp getTimestamp(String name) throws SQLException {
    checkName(name);
    java.util.Date date = currentRow.getTimestamp(name);
    if (date == null) {
      return null;
    }
    return new Timestamp(currentRow.getTimestamp(name).getTime());
  }

  public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException {
    checkName(name);
    // silently ignore the Calendar argument; its a hint we do not need
    return getTimestamp(name);
  }


  public int getType() throws SQLException {
    checkNotClosed();
    return resultSetType;
  }

  public URL getURL(int arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public URL getURL(String arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public SQLWarning getWarnings() throws SQLException {
    checkNotClosed();
    return null;
  }


  public boolean isAfterLast() throws SQLException {
    checkNotClosed();
    return rowNumber == Integer.MAX_VALUE;
  }

  public boolean isBeforeFirst() throws SQLException {
    checkNotClosed();
    return rowNumber == 0;
  }

  public boolean isClosed() {
    if (this.statement == null) {
      return true;
    }
    return this.statement.isClosed();
  }

  public boolean isFirst() throws SQLException {
    checkNotClosed();
    return rowNumber == 1;
  }

  public boolean isLast() throws SQLException {
    checkNotClosed();
    return !rowsIterator.hasNext();
  }

  public boolean isWrapperFor(Class<?> iface) {
    return CassandraResultSetExtras.class.isAssignableFrom(iface);
  }

  // Navigation between rows within the returned set of rows
  // Need to use a list iterator so next() needs completely re-thought

  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public synchronized boolean next() {
    if (hasMoreRows()) {
      if (rowNumber != 0) {
        populateColumns();
      }
      rowNumber++;
      return true;
    }
    rowNumber = Integer.MAX_VALUE;
    return false;
  }


  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  public boolean relative(int arg0) throws SQLException {
    throw new SQLFeatureNotSupportedException(NOT_SUPPORTED);
  }

  @SuppressWarnings("boxing")
  public void setFetchDirection(int direction) throws SQLException {
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
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.equals(CassandraResultSetExtras.class)) {
      return (T) this;
    }

    throw new SQLFeatureNotSupportedException(String.format(NO_INTERFACE, iface.getSimpleName()));
  }

  public boolean wasNull() {
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
      assert statement != null;
      return statement.connection.getCatalog();
    }

    public String getColumnClassName(int column) {
      if (currentRow != null) {
        return DataTypeEnum.fromCqlTypeName(currentRow.getColumnDefinitions().getType(column - 1).getName()).asJavaClass().getCanonicalName();
      }
      return DataTypeEnum.fromCqlTypeName(driverResultSet.getColumnDefinitions().asList().get(column - 1).getType().getName()).asJavaClass().getCanonicalName();

    }

    public int getColumnCount() {
      try {
        if (currentRow != null) {
          return currentRow.getColumnDefinitions().size();
        }
        return driverResultSet.getColumnDefinitions().size();
      } catch (Exception e) {
        return 0;
      }
    }

    @SuppressWarnings("rawtypes")
    public int getColumnDisplaySize(int column) {
      //checkIndex(column);
      Definition col;
      if (currentRow != null) {
        col = currentRow.getColumnDefinitions().asList().get(column - 1);
      } else {
        col = driverResultSet.getColumnDefinitions().asList().get(column - 1);
      }
      try {

        int length = -1;
        AbstractJdbcType jtype = TypesMap.getTypeForComparator(col.getType().toString());
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
        return length;
      } catch (Exception e) {
        return -1;
      }
    }

    public String getColumnLabel(int column) {
      return getColumnName(column);
    }

    public String getColumnName(int column) {
      //checkIndex(column);

      if (currentRow != null) {
        return currentRow.getColumnDefinitions().getName(column - 1);
      }
      return driverResultSet.getColumnDefinitions().asList().get(column - 1).getName();
    }

    public int getColumnType(int column) {
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
    public String getColumnTypeName(int column) {
      DataType type;
      if (currentRow != null) {
        type = currentRow.getColumnDefinitions().getType(column - 1);
      } else {
        type = driverResultSet.getColumnDefinitions().getType(column - 1);
      }

      return type.toString();
    }

    public int getPrecision(int column) {
      return 0;
    }

    public int getScale(int column) {
      return 0;
    }

    /**
     * return the DEFAULT current Keyspace as the Schema Name
     */
    public String getSchemaName(int column) throws SQLException {
      assert statement != null;
      return statement.connection.getSchema();
    }

    public boolean isAutoIncrement(int column) {
      return true;
    }

    public boolean isCaseSensitive(int column) {
      return true;
    }

    public boolean isCurrency(int column) {
      return false;
    }

    public boolean isDefinitelyWritable(int column) {
      return isWritable(column);
    }

    /**
     * absence is the equivalent of null in Cassandra
     */
    public int isNullable(int column) {
      return ResultSetMetaData.columnNullable;
    }

    public boolean isReadOnly(int column) {
      return column == 0;
    }

    public boolean isSearchable(int column) {
      return false;
    }

    public boolean isSigned(int column) {
      return false;
    }

    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }

    public boolean isWritable(int column) {
      return column > 0;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
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
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    checkIndex(columnIndex);
    byte[] bytes = new byte[currentRow.getBytes(columnIndex - 1).remaining()];
    currentRow.getBytes(columnIndex - 1).get(bytes, 0, bytes.length);
    return new ByteArrayInputStream(bytes);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
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
