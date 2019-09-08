//package org.apache.cassandra2.cql.jdbc;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//public class MainClass {
//
//  public static void main(String[] args) throws ClassNotFoundException, SQLException {
//    Class.forName("org.apache.cassandra2.cql.jdbc.CassandraDriver");
//    Connection conn = DriverManager.getConnection("jdbc:cassandra://127.0.0.1:9042/yantriks_yso", "cassandra", "cassandra");
//    Statement stmt = conn.createStatement();
//    stmt.execute("SELECT * FROM yantriks_capacity.ycs_location_fulfillment_type");
//    ResultSet rs = stmt.getResultSet();
//    int columnsSize = rs.getMetaData().getColumnCount();
//    for (int i=0; i < columnsSize; i++) {
//      System.out.println(rs.getMetaData().getColumnName(i+1));
//      System.out.println(rs.getMetaData().getColumnType(i+1));
//      System.out.println(rs.getMetaData().getColumnTypeName(i+1));
//    }
//    System.out.println(rs.getObject("capacity_reset_time"));
//    rs.close();
//    stmt.close();
//    conn.close();
//  }
//}
