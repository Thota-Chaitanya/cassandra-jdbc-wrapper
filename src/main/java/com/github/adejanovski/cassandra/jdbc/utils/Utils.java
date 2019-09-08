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

package com.github.adejanovski.cassandra.jdbc.utils;

import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy.Builder;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of static utility methods used by the JDBC Suite, and various default values and error message strings that can be shared across classes.
 */
public class Utils {

  public static final String PROTOCOL = "jdbc:cassandra:";
  private static final int DEFAULT_PORT = 9042;
  private static final String KEY_VERSION = "version";
  private static final String KEY_CONSISTENCY = "consistency";
  private static final String KEY_PRIMARY_DC = "primarydc";
  private static final String KEY_BACKUP_DC = "backupdc";
  private static final String KEY_CONNECTION_RETRIES = "retries";
  private static final String KEY_LOADBALANCING_POLICY = "loadbalancing";
  private static final String KEY_RETRY_POLICY = "retry";
  private static final String KEY_RECONNECT_POLICY = "reconnection";
  private static final String KEY_DEBUG = "debug";
  public static final String TAG_USER = "user";
  public static final String TAG_PASSWORD = "password";
  public static final String TAG_DATABASE_NAME = "databaseName";
  public static final String TAG_SERVER_NAME = "serverName";
  public static final String TAG_PORT_NUMBER = "portNumber";
  public static final String TAG_ACTIVE_CQL_VERSION = "activeCqlVersion";
  public static final String TAG_CQL_VERSION = "cqlVersion";
  public static final String TAG_CONSISTENCY_LEVEL = "consistencyLevel";
  public static final String TAG_LOADBALANCING_POLICY = "loadBalancing";
  public static final String TAG_RETRY_POLICY = "retry";
  public static final String TAG_RECONNECT_POLICY = "reconnection";
  public static final String TAG_DEBUG = "debug";
  public static final String TAG_PRIMARY_DC = "primaryDatacenter";
  private static final String TAG_BACKUP_DC = "backupDatacenter";
  private static final String TAG_CONNECTION_RETRIES = "retries";
  public static final String NOT_SUPPORTED = "the Cassandra implementation does not support this method";
  public static final String WAS_CLOSED_CON = "method was called on a closed Connection";
  public static final String WAS_CLOSED_STMT = "method was called on a closed Statement";
  public static final String WAS_CLOSED_RSLT = "method was called on a closed ResultSet";
  public static final String NO_INTERFACE = "no object was found that matched the provided interface: %s";
  public static final String NO_TRANSACTIONS = "the Cassandra implementation does not support transactions";
  public static final String ALWAYS_AUTOCOMMIT = "the Cassandra implementation is always in auto-commit mode";
  public static final String BAD_TIMEOUT = "the timeout value was less than zero";
  public static final String NO_GEN_KEYS = "the Cassandra implementation does not currently support returning generated  keys";
  public static final String NO_MULTIPLE = "the Cassandra implementation does not currently support multiple open Result Sets";
  public static final String NO_RESULTSET = "No ResultSet returned from the CQL statement passed in an 'executeQuery()' method";
  public static final String BAD_KEEP_RSET = "the argument for keeping the current result set : %s is not a valid value";
  public static final String BAD_TYPE_RSET = "the argument for result set type : %s is not a valid value";
  public static final String BAD_HOLD_RSET = "the argument for result set holdability : %s is not a valid value";
  public static final String BAD_FETCH_DIR = "fetch direction value of : %s is illegal";
  public static final String BAD_AUTO_GEN = "auto key generation value of : %s is illegal";
  public static final String BAD_FETCH_SIZE = "fetch size of : %s rows may not be negative";
  public static final String MUST_BE_POSITIVE = "index must be a positive number less or equal the count of returned columns: %s";
  public static final String VALID_LABELS = "name provided was not in the list of valid column labels: %s";
  private static final String HOST_IN_URL = "Connection url must specify a host, e.g., jdbc:cassandra://localhost:9042/Keyspace1";
  public static final String HOST_REQUIRED = "a 'host' name is required to build a Connection";
  private static final String BAD_KEYSPACE = "Keyspace names must be composed of alphanumerics and underscores (parsed: '%s')";
  private static final String URI_IS_SIMPLE = "Connection url may only include host, port, and keyspace, consistency and version option, e.g., jdbc:cassandra://localhost:9042/Keyspace1?version=3.0.0&consistency=ONE";
  public static final String FORWARD_ONLY = "Can not position cursor with a type of TYPE_FORWARD_ONLY";
  protected static final Logger logger = LoggerFactory.getLogger(Utils.class);

  /**
   * Parse a URL for the Cassandra JDBC Driver
   * <p></p>
   * The URL must start with the Protocol: "jdbc:cassandra:" The URI part(the "Subname") must contain a host and an optional port and optional keyspace name ie.
   * "//localhost:9160/Test1"
   *
   * @param url The full JDBC URL to be parsed
   * @return A list of properties that were parsed from the Subname
   */
  public static Properties parseURL(String url) throws SQLException {
    Properties props = new Properties();

    if (!(url == null)) {
      props.setProperty(TAG_PORT_NUMBER, "" + DEFAULT_PORT);
      String rawUri = url.substring(PROTOCOL.length());
      URI uri;
      try {
        uri = new URI(rawUri);
      } catch (URISyntaxException e) {
        throw new SQLSyntaxErrorException(e);
      }

      String host = uri.getHost();
      if (host == null) {
        throw new SQLNonTransientConnectionException(HOST_IN_URL);
      }
      props.setProperty(TAG_SERVER_NAME, host);

      int port = uri.getPort() >= 0 ? uri.getPort() : DEFAULT_PORT;
      props.setProperty(TAG_PORT_NUMBER, "" + port);

      String keyspace = uri.getPath();
      if ((keyspace != null) && (!keyspace.isEmpty())) {
        if (keyspace.startsWith("/")) {
          keyspace = keyspace.substring(1);
        }
        if (!keyspace.matches("[a-zA-Z]\\w+")) {
          throw new SQLNonTransientConnectionException(String.format(BAD_KEYSPACE, keyspace));
        }
        props.setProperty(TAG_DATABASE_NAME, keyspace);
      }

      if (uri.getUserInfo() != null) {
        throw new SQLNonTransientConnectionException(URI_IS_SIMPLE);
      }

      String query = uri.getQuery();
      if ((query != null) && (!query.isEmpty())) {
        Map<String, String> params = parseQueryPart(query);
        if (params.containsKey(KEY_VERSION)) {
          props.setProperty(TAG_CQL_VERSION, params.get(KEY_VERSION));
        }
        if (params.containsKey(KEY_DEBUG)) {
          props.setProperty(TAG_DEBUG, params.get(KEY_DEBUG));
        }
        if (params.containsKey(KEY_CONSISTENCY)) {
          props.setProperty(TAG_CONSISTENCY_LEVEL, params.get(KEY_CONSISTENCY));
        }
        if (params.containsKey(KEY_PRIMARY_DC)) {
          props.setProperty(TAG_PRIMARY_DC, params.get(KEY_PRIMARY_DC));
        }
        if (params.containsKey(KEY_BACKUP_DC)) {
          props.setProperty(TAG_BACKUP_DC, params.get(KEY_BACKUP_DC));
        }
        if (params.containsKey(KEY_CONNECTION_RETRIES)) {
          props.setProperty(TAG_CONNECTION_RETRIES, params.get(KEY_CONNECTION_RETRIES));
        }
        if (params.containsKey(KEY_LOADBALANCING_POLICY)) {
          props.setProperty(TAG_LOADBALANCING_POLICY, params.get(KEY_LOADBALANCING_POLICY));
        }
        if (params.containsKey(KEY_RETRY_POLICY)) {
          props.setProperty(TAG_RETRY_POLICY, params.get(KEY_RETRY_POLICY));
        }
        if (params.containsKey(KEY_RECONNECT_POLICY)) {
          props.setProperty(TAG_RECONNECT_POLICY, params.get(KEY_RECONNECT_POLICY));
        }


      }
    }

    if (logger.isTraceEnabled()) {
      logger.trace("URL : '{}' parses to: {}", url, props);
    }

    return props;
  }

  /**
   * Create a "Subname" portion of a JDBC URL from properties.
   *
   * @param props A Properties file containing all the properties to be considered.
   * @return A constructed "Subname" portion of a JDBC URL in the form of a CLI (ie: //myhost:9160/Test1?version=3.0.0 )
   */
  public static String createSubName(Properties props) throws SQLException {
    // make keyspace always start with a "/" for URI
    String keyspace = props.getProperty(TAG_DATABASE_NAME);

    // if keyspace is null then do not bother ...
    if (keyspace != null) {
      if (!keyspace.startsWith("/")) {
        keyspace = "/" + keyspace;
      }
    }

    String host = props.getProperty(TAG_SERVER_NAME);
    if (host == null) {
      throw new SQLNonTransientConnectionException(HOST_REQUIRED);
    }

    // construct a valid URI from parts...
    URI uri;
    try {
      uri = new URI(
          null,
          null,
          host,
          props.getProperty(TAG_PORT_NUMBER) == null ? DEFAULT_PORT : Integer.parseInt(props.getProperty(TAG_PORT_NUMBER)),
          keyspace,
          makeQueryString(props),
          null);
    } catch (Exception e) {
      throw new SQLNonTransientConnectionException(e);
    }

    if (logger.isTraceEnabled()) {
      logger.trace("Subname : '{}' created from : {}", uri.toString(), props);
    }

    return uri.toString();
  }

  private static String makeQueryString(Properties props) {
    StringBuilder sb = new StringBuilder();
    String version = (props.getProperty(TAG_CQL_VERSION));
    String consistency = (props.getProperty(TAG_CONSISTENCY_LEVEL));
    if (consistency != null) {
      sb.append(KEY_CONSISTENCY).append("=").append(consistency);
    }
    if (version != null) {
      if (sb.length() != 0) {
        sb.append("&");
      }
      sb.append(KEY_VERSION).append("=").append(version);
    }

    return (sb.length() == 0) ? null : sb.toString().trim();
  }

  private static Map<String, String> parseQueryPart(String query) throws SQLException {
    Map<String, String> params = new HashMap<>();
    for (String param : query.split("&")) {
      try {
        String[] pair = param.split("=");
        String key = URLDecoder.decode(pair[0], "UTF-8").toLowerCase();
        String value = "";
        if (pair.length > 1) {
          value = URLDecoder.decode(pair[1], "UTF-8");
        }
        params.put(key, value);
      } catch (UnsupportedEncodingException e) {
        throw new SQLSyntaxErrorException(e);
      }
    }
    return params;
  }

  public static LinkedHashSet<?> parseSet(String itemType, String value) {

    String[] split = value.replace("[", "").replace("]", "").split(", ");
    switch (itemType) {
      case "varchar":
      case "text":
      case "ascii":
        return new LinkedHashSet<>(Arrays.asList(split));

      case "bigint": {
        LinkedHashSet<Long> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(Long.parseLong(val.trim()));
        }
        return zeSet;
      }
      case "varint": {
        LinkedHashSet<BigInteger> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(BigInteger.valueOf(Long.parseLong(val.trim())));
        }
        return zeSet;
      }
      case "decimal": {
        LinkedHashSet<BigDecimal> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(BigDecimal.valueOf(Double.parseDouble(val.trim())));
        }
        return zeSet;
      }
      case "double": {
        LinkedHashSet<Double> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(Double.parseDouble(val.trim()));
        }
        return zeSet;
      }
      case "float": {
        LinkedHashSet<Float> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(Float.parseFloat(val.trim()));
        }
        return zeSet;
      }
      case "boolean": {
        LinkedHashSet<Boolean> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(Boolean.parseBoolean(val.trim()));
        }
        return zeSet;
      }
      case "int": {
        LinkedHashSet<Integer> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(Integer.parseInt(val.trim()));
        }
        return zeSet;
      }
      case "uuid":
      case "timeuuid": {
        LinkedHashSet<UUID> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(UUID.fromString(val.trim()));
        }
        return zeSet;
      }
      case "smallint": {
        LinkedHashSet<Short> zeSet = Sets.newLinkedHashSet();

        for (String val : split) {
          zeSet.add(Short.parseShort(val.trim()));
        }
        return zeSet;
      }
    }
    return null;

  }

  public static ArrayList<?> parseList(String itemType, String value) {

    String[] split = value.replace("[", "").replace("]", "").split(", ");
    switch (itemType) {
      case "varchar":
      case "text":
      case "ascii": {
        ArrayList<String> zeList = Lists.newArrayList();
        int i = 0;
        for (String val : split) {
          if (i > 0 && val.startsWith(" ")) {
            zeList.add(val.substring(1));
          } else {
            zeList.add(val);
          }
          i++;
        }
        return zeList;

      }
      case "bigint": {
        ArrayList<Long> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(Long.parseLong(val.trim()));
        }
        return zeList;
      }
      case "varint": {
        ArrayList<BigInteger> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(BigInteger.valueOf(Long.parseLong(val.trim())));
        }
        return zeList;
      }
      case "decimal": {
        ArrayList<BigDecimal> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(BigDecimal.valueOf(Double.parseDouble(val.trim())));
        }
        return zeList;
      }
      case "double": {
        ArrayList<Double> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(Double.parseDouble(val.trim()));
        }
        return zeList;
      }
      case "float": {
        ArrayList<Float> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(Float.parseFloat(val.trim()));
        }
        return zeList;
      }
      case "boolean": {
        ArrayList<Boolean> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(Boolean.parseBoolean(val.trim()));
        }
        return zeList;
      }
      case "int": {
        ArrayList<Integer> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(Integer.parseInt(val.trim()));
        }
        return zeList;
      }
      case "uuid":
      case "timeuuid": {
        ArrayList<UUID> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(UUID.fromString(val.trim()));
        }
        return zeList;
      }
      case "smallint": {
        ArrayList<Short> zeList = Lists.newArrayList();

        for (String val : split) {
          zeList.add(Short.parseShort(val.trim()));
        }
        return zeList;
      }
    }
    return null;

  }

  @SuppressWarnings({"boxing", "unchecked", "rawtypes"})
  public static HashMap<?, ?> parseMap(String kType, String vType, String value) {
    //Parsing values looking like this :
    //{key1:val1, key2:val2}

    Map zeMap = new HashMap();
    String[] values = value.replace("{", "").replace("}", "").split(", ");
    List keys = Lists.newArrayList();
    List vals = Lists.newArrayList();

    for (String val : values) {
      String[] keyVal = val.split("=");
      switch (kType) {
        case "bigint":
          keys.add(Long.parseLong(keyVal[0]));
          break;
        case "varint":
          keys.add(BigInteger.valueOf(Long.parseLong(keyVal[0])));
          break;
        case "decimal":
          keys.add(BigDecimal.valueOf(Double.parseDouble(keyVal[0])));
          break;
        case "double":
          keys.add(Double.parseDouble(keyVal[0]));
          break;
        case "float":
          keys.add(Float.parseFloat(keyVal[0]));
          break;
        case "boolean":
          keys.add(Boolean.parseBoolean(keyVal[0]));
          break;
        case "int":
          keys.add(Integer.parseInt(keyVal[0]));
          break;
        case "uuid":
        case "timeuuid":
          keys.add(UUID.fromString(keyVal[0]));
          break;
        case "smallint":
          keys.add(Short.parseShort(keyVal[0]));
          break;
        default:
          keys.add(keyVal[0]);
          break;
      }

      switch (vType) {
        case "bigint":
          vals.add(Long.parseLong(keyVal[1]));
          break;
        case "varint":
          vals.add(BigInteger.valueOf(Long.parseLong(keyVal[1])));
          break;
        case "decimal":
          vals.add(BigDecimal.valueOf(Double.parseDouble(keyVal[1])));
          break;
        case "double":
          vals.add(Double.parseDouble(keyVal[1]));
          break;
        case "float":
          vals.add(Float.parseFloat(keyVal[1]));
          break;
        case "boolean":
          vals.add(Boolean.parseBoolean(keyVal[1]));
          break;
        case "int":
          vals.add(Integer.parseInt(keyVal[1]));
          break;
        case "uuid":
        case "timeuuid":
          vals.add(UUID.fromString(keyVal[1]));
          break;
        case "smallint":
          vals.add(Short.parseShort(keyVal[1]));
          break;
        default:
          vals.add(keyVal[1]);
          break;
      }

      zeMap.put(keys.get(keys.size() - 1), vals.get(vals.size() - 1));

    }

    return (HashMap<?, ?>) zeMap;

  }

  public static LoadBalancingPolicy parseLbPolicy(String loadBalancingPolicyString)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
    String lb_regex = "([a-zA-Z.]*Policy)(\\()(.*)(\\))";
    Pattern lb_pattern = Pattern.compile(lb_regex);
    Matcher lb_matcher = lb_pattern.matcher(loadBalancingPolicyString);

    if (lb_matcher.matches()) {
      if (lb_matcher.groupCount() > 0) {
        // Primary LB policy has been specified
        String primaryLoadBalancingPolicy = lb_matcher.group(1);
        String loadBalancingPolicyParams = lb_matcher.group(3);
        return getLbPolicy(primaryLoadBalancingPolicy, loadBalancingPolicyParams);
      }
    }

    return null;
  }

  private static LoadBalancingPolicy getLbPolicy(String lbString, String parameters)
      throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    LoadBalancingPolicy policy;
    if (!lbString.contains(".")) {
      if (lbString.toLowerCase().contains("dcawareroundrobinpolicy")) {
        lbString = "com.github.adejanovski.cassandra.jdbc.policies." + lbString;
      } else {
        lbString = "com.datastax.driver.core.policies." + lbString;
      }
    }

    if (parameters.length() > 0) {
      System.out.println("parameters = " + parameters);
      String paramsRegex = "([^,]+\\(.+?\\))|([^,]+)";
      Pattern param_pattern = Pattern.compile(paramsRegex);
      Matcher lb_matcher = param_pattern.matcher(parameters);

      ArrayList<Object> paramList = Lists.newArrayList();
      ArrayList<Class> primaryParametersClasses = Lists.newArrayList();
      int nb = 0;
      while (lb_matcher.find()) {
        if (lb_matcher.groupCount() > 0) {
          try {
            if (lb_matcher.group().contains("(") && !lb_matcher.group().trim().startsWith("(")) {
              // We are dealing with child policies here
              primaryParametersClasses.add(LoadBalancingPolicy.class);
              // Parse and add child policy to the parameter list
              paramList.add(parseLbPolicy(lb_matcher.group()));
              nb++;
            } else {
              // We are dealing with parameters that are not policies here
              String param = lb_matcher.group();
              if (param.contains("'") || param.contains("\"")) {
                primaryParametersClasses.add(String.class);
                paramList.add(param.trim().replace("'", "").replace("\"", ""));
              } else if (param.contains(".") || param.toLowerCase().contains("(double)") || param.toLowerCase().contains("(float)")) {
                // gotta allow using float or double
                if (param.toLowerCase().contains("(double)")) {
                  primaryParametersClasses.add(double.class);
                  paramList.add(Double.parseDouble(param.replace("(double)", "").trim()));
                } else {
                  primaryParametersClasses.add(float.class);
                  paramList.add(Float.parseFloat(param.replace("(float)", "").trim()));
                }
              } else {
                if (param.toLowerCase().contains("(long)")) {
                  primaryParametersClasses.add(long.class);
                  paramList.add(Long.parseLong(param.toLowerCase().replace("(long)", "").trim()));
                } else {
                  primaryParametersClasses.add(int.class);
                  paramList.add(Integer.parseInt(param.toLowerCase().replace("(int)", "").trim()));
                }
              }
              nb++;
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }

      if (nb > 0) {
        // Instantiate load balancing policy with parameters
        if (lbString.toLowerCase().contains("latencyawarepolicy")) {
          //special sauce for the latency aware policy which uses a builder subclass to instantiate
          Builder builder = LatencyAwarePolicy.builder((LoadBalancingPolicy) paramList.get(0));

          builder.withExclusionThreshold((Double) paramList.get(1));
          builder.withScale((Long) paramList.get(2), TimeUnit.MILLISECONDS);
          builder.withRetryPeriod((Long) paramList.get(3), TimeUnit.MILLISECONDS);
          builder.withUpdateRate((Long) paramList.get(4), TimeUnit.MILLISECONDS);
          builder.withMininumMeasurements((Integer) paramList.get(5));

          return builder.build();

        } else {
          Class<?> clazz = Class.forName(lbString);
          Constructor<?> constructor = clazz.getConstructor(primaryParametersClasses.toArray(new Class[0]));
          if (lbString.toLowerCase().contains("dcawareroundrobinpolicy")) {
            // special sauce for the DCAwareRRPolicy that has no more public constructor in 3.0 and now uses the builder pattern
            com.github.adejanovski.cassandra.jdbc.policies.DCAwareRoundRobinPolicy wrappedPolicy = (com.github.adejanovski.cassandra.jdbc.policies.DCAwareRoundRobinPolicy) constructor
                .newInstance(paramList.toArray(new Object[0]));
            return wrappedPolicy.build();
          } else {
            return (LoadBalancingPolicy) constructor.newInstance(paramList.toArray(new Object[0]));
          }
        }
      } else {
        // Only one policy has been specified, with no parameter or child policy
        Class<?> clazz = Class.forName(lbString);
        policy = (LoadBalancingPolicy) clazz.newInstance();

        return policy;

      }

    } else {
      // Only one policy has been specified, with no parameter or child policy

      Class<?> clazz = Class.forName(lbString);
      policy = (LoadBalancingPolicy) clazz.newInstance();

      return policy;

    }
  }

  public static RetryPolicy parseRetryPolicy(String retryPolicyString)
      throws IllegalAccessException, ClassNotFoundException, SecurityException, IllegalArgumentException, NoSuchFieldException {

    if (!retryPolicyString.contains(".")) {
      retryPolicyString = "com.datastax.driver.core.policies." + retryPolicyString;

      Class<?> clazz = Class.forName(retryPolicyString);

      Field field = clazz.getDeclaredField("INSTANCE");

      return (RetryPolicy) field.get(null);
    }

    return null;
  }


  public static ReconnectionPolicy parseReconnectionPolicy(String reconnectionPolicyString)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, SecurityException, IllegalArgumentException, InvocationTargetException {
    String lb_regex = "([a-zA-Z.]*Policy)(\\()(.*)(\\))";
    Pattern lb_pattern = Pattern.compile(lb_regex);
    Matcher lb_matcher = lb_pattern.matcher(reconnectionPolicyString);

    if (lb_matcher.matches()) {
      if (lb_matcher.groupCount() > 0) {
        // Primary LB policy has been specified
        String primaryReconnectionPolicy = lb_matcher.group(1);
        String reconnectionPolicyParams = lb_matcher.group(3);
        return getReconnectionPolicy(primaryReconnectionPolicy, reconnectionPolicyParams);
      }
    }

    return null;
  }

  private static ReconnectionPolicy getReconnectionPolicy(String rcString, String parameters)
      throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    ReconnectionPolicy policy;
    if (!rcString.contains(".")) {
      rcString = "com.datastax.driver.core.policies." + rcString;
    }

    if (parameters.length() > 0) {
      // Child policy or parameters have been specified
      System.out.println("parameters = " + parameters);
      String paramsRegex = "([^,]+\\(.+?\\))|([^,]+)";
      Pattern param_pattern = Pattern.compile(paramsRegex);
      Matcher lb_matcher = param_pattern.matcher(parameters);

      ArrayList<Object> paramList = Lists.newArrayList();
      ArrayList<Class> primaryParametersClasses = Lists.newArrayList();
      int nb = 0;
      while (lb_matcher.find()) {
        if (lb_matcher.groupCount() > 0) {
          try {
            if (lb_matcher.group().contains("(") && !lb_matcher.group().trim().startsWith("(")) {
              // We are dealing with child policies here
              primaryParametersClasses.add(LoadBalancingPolicy.class);
              // Parse and add child policy to the parameter list
              paramList.add(parseReconnectionPolicy(lb_matcher.group()));
              nb++;
            } else {
              // We are dealing with parameters that are not policies here
              String param = lb_matcher.group();
              if (param.contains("'")) {
                primaryParametersClasses.add(String.class);
                paramList.add(param.trim().replace("'", ""));
              } else if (param.contains(".") || param.toLowerCase().contains("(double)") || param.toLowerCase().contains("(float)")) {
                // gotta allow using float or double
                if (param.toLowerCase().contains("(double)")) {
                  primaryParametersClasses.add(double.class);
                  paramList.add(Double.parseDouble(param.replace("(double)", "").trim()));
                } else {
                  primaryParametersClasses.add(float.class);
                  paramList.add(Float.parseFloat(param.replace("(float)", "").trim()));
                }
              } else {
                if (param.toLowerCase().contains("(long)")) {
                  primaryParametersClasses.add(long.class);
                  paramList.add(Long.parseLong(param.toLowerCase().replace("(long)", "").trim()));
                } else {
                  primaryParametersClasses.add(int.class);
                  paramList.add(Integer.parseInt(param.toLowerCase().replace("(int)", "").trim()));
                }
              }
              nb++;
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }

      if (nb > 0) {
        // Instantiate load balancing policy
        // with parameters
        Class<?> clazz = Class.forName(rcString);
        Constructor<?> constructor = clazz.getConstructor(primaryParametersClasses.toArray(new Class[0]));

        return (ReconnectionPolicy) constructor.newInstance(paramList.toArray(new Object[0]));

      }
      // Only one policy has been specified, with no parameter or child policy
      Class<?> clazz = Class.forName(rcString);
      policy = (ReconnectionPolicy) clazz.newInstance();

      return policy;

    }
    Class<?> clazz = Class.forName(rcString);
    policy = (ReconnectionPolicy) clazz.newInstance();

    return policy;
  }

}
