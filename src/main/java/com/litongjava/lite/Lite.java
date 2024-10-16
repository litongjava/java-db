package com.litongjava.lite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.litongjava.tio.utils.snowflake.SnowflakeIdUtils;

public class Lite {
  public static final String sql_statement_stat = "sql_statement_stat";
  public static final String DB_URL = "jdbc:sqlite:default_database.db";
  public static final String driverClass = "org.sqlite.JDBC";

  // SQL to create table if it doesn't exist
  private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS " + sql_statement_stat + " (" +
  //
      "id LONG PRIMARY KEY, " +
      //
      "name TEXT, " +
      //
      "sqlType TEXT, " +
      //
      "sql TEXT, " +
      //
      "paras TEXT, " +
      //
      "rows INTEGER, " +
      //
      "startTimeMillis LONG, " +
      //
      "elapsed LONG)";

  static {
    // Load the SQLite JDBC driver
    try {
      Class.forName(driverClass);
    } catch (ClassNotFoundException e1) {
      e1.printStackTrace();
    }

    try (Connection conn = DriverManager.getConnection(DB_URL)) {
      if (!isTableExists(conn, sql_statement_stat)) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(CREATE_TABLE_SQL);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  // Method to check if table exists
  private static boolean isTableExists(Connection conn, String tableName) throws SQLException {
    String checkTableExistsSQL = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
    try (PreparedStatement pstmt = conn.prepareStatement(checkTableExistsSQL)) {
      pstmt.setString(1, tableName);
      try (ResultSet rs = pstmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  public static void saveSqlStatementStat(String name, String sqlType, String sql, Object[] paras, int result, long startTimeMillis, long elapsed) {
    String insertSQL = "INSERT INTO " + sql_statement_stat + " (id, name, sqlType, sql, paras, rows, startTimeMillis, elapsed) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(DB_URL); PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

      long id = SnowflakeIdUtils.id();
      pstmt.setLong(1, id);
      pstmt.setString(2, name);
      pstmt.setString(3, sqlType);
      pstmt.setString(4, sql);
      pstmt.setString(5, paras != null ? Arrays.toString(paras) : "");
      pstmt.setInt(6, result);
      pstmt.setLong(7, startTimeMillis);
      pstmt.setLong(8, elapsed);

      pstmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void saveSqlStatementStat(String name, String sqlType, String sql, @SuppressWarnings("rawtypes") List paras, int result, long startTimeMillis, long elapsed) {
    String insertSQL = "INSERT INTO " + sql_statement_stat + " (id, name, sqlType, sql, paras, rows, startTimeMillis, elapsed) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    try (Connection conn = DriverManager.getConnection(DB_URL); PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

      long id = SnowflakeIdUtils.id();
      pstmt.setLong(1, id);
      pstmt.setString(2, name);
      pstmt.setString(3, sqlType);
      pstmt.setString(4, sql);
      pstmt.setString(5, paras != null ? paras.toString() : "");
      pstmt.setInt(6, result);
      pstmt.setLong(7, startTimeMillis);
      pstmt.setLong(8, elapsed);

      pstmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static List<Map<String, Object>> querySqlStatementStats() {
    List<Map<String, Object>> results = new ArrayList<>();
    String querySQL = "SELECT * FROM " + sql_statement_stat;

    try (Connection conn = DriverManager.getConnection(DB_URL); Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(querySQL)) {

      while (rs.next()) {
        Map<String, Object> record = new HashMap<>();
        record.put("id", rs.getLong("id"));
        record.put("name", rs.getString("name"));
        record.put("sql_type", rs.getString("sqlType"));
        record.put("sql", rs.getString("sql"));
        record.put("params", rs.getString("paras"));
        record.put("rows", rs.getInt("rows"));
        record.put("start_time", rs.getLong("startTimeMillis"));
        record.put("elapsed", rs.getLong("elapsed"));
        results.add(record);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    return results;
  }
}
