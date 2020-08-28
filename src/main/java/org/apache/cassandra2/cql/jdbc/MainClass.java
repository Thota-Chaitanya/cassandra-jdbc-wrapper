//package org.apache.cassandra2.cql.jdbc;
//
//import com.datastax.driver.core.LocalDate;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.time.LocalDateTime;
//import java.time.LocalTime;
//
//public class MainClass {
//
//  public static void main(String[] args) throws ClassNotFoundException, SQLException {
//    Class.forName("org.apache.cassandra2.cql.jdbc.CassandraDriver");
//    Connection conn = DriverManager.getConnection("jdbc:cassandra://127.0.0.1:9042/yantriks", "cassandra", "cassandra");
//    Statement stmt = conn.createStatement();
//    stmt.execute("select * from yantriks_capacity.ycs_location_fulfillment_type_override where org_id='PETCOUS' and location_type='DC' and location_id='101' and fulfillment_type='SHIP' and date='2019-11-24' and selling_channel='Digital'");
//    ResultSet rs = stmt.getResultSet();
////    int columnsSize = rs.getMetaData().getColumnCount();
////    for (int i=0; i < columnsSize; i++) {
////      System.out.println(rs.getMetaData().getColumnName(i+1));
////      System.out.println(rs.getMetaData().getColumnType(i+1));
////      System.out.println(rs.getMetaData().getColumnTypeName(i+1));
////    }
////
////    System.out.println(LocalDateTime. (86399000000000L));
//     System.out.println(rs.getDate("date"));
//    rs.close();
//    stmt.close();
//    conn.close();
//  }
//}