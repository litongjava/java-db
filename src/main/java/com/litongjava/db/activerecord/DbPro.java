package com.litongjava.db.activerecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.postgresql.util.PGobject;

import com.jfinal.kit.StrKit;
import com.jfinal.kit.TimeKit;
import com.litongjava.cache.IDbCache;
import com.litongjava.db.SqlPara;
import com.litongjava.db.activerecord.stat.ISqlStatementStat;
import com.litongjava.model.db.IAtom;
import com.litongjava.model.db.ICallback;
import com.litongjava.model.page.Page;

import lombok.extern.slf4j.Slf4j;

/**
 * DbPro. Professional database query and update tool.
 */
@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DbPro {

  private String queryColumnByField = "select %s from %s where %s=?";

  public final Config config;

  public DbPro() {
    if (DbKit.config == null) {
      throw new ActiveRecordException("The main config is null, initialize ActiveRecordPlugin first");
    }
    this.config = DbKit.config;
  }

  public DbPro(String configName) {
    this.config = DbKit.getConfig(configName);
    if (this.config == null) {
      throw new IllegalArgumentException("Config not found by configName: " + configName);
    }
  }

  public Config getConfig() {
    return config;
  }

  public List<byte[]> queryListBytes(Config config, Connection conn, String sql, Object... paras) {
    List<byte[]> result = new ArrayList();
    PreparedStatement pst = null;
    try {
      pst = conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    try {
      config.dialect.fillStatement(pst, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    long start = System.currentTimeMillis();

    ResultSet rs = null;
    try {
      rs = pst.executeQuery();
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    int colAmount = 0;
    try {
      colAmount = rs.getMetaData().getColumnCount();
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    if (colAmount > 1) {
      throw new ActiveRecordException("please use queryListMultiBytes");
    } else if (colAmount == 1) {
      try {
        while (rs.next()) {
          result.add(rs.getBytes(1));
        }
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, paras, e);
      }
    }

    ISqlStatementStat stat = config.getSqlStatementStat();
    if (stat != null) {
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      stat.save(config.name, "query", sql, paras, result.size(), start, elapsed, config.writeSync);
    }
    return result;
  }

  public <T> List<T> query(Config config, Connection conn, String sql, Object... paras) {
    List result = new ArrayList();
    try (PreparedStatement pst = conn.prepareStatement(sql)) {
      config.dialect.fillStatement(pst, paras);
      long start = System.currentTimeMillis();
      try (ResultSet rs = pst.executeQuery()) {

        int colAmount = rs.getMetaData().getColumnCount();
        if (colAmount > 1) {
          while (rs.next()) {
            Object[] temp = new Object[colAmount];
            for (int i = 0; i < colAmount; i++) {
              temp[i] = rs.getObject(i + 1);
            }
            result.add(temp);
          }
        } else if (colAmount == 1) {
          while (rs.next()) {
            result.add(rs.getObject(1));
          }
        }
        ISqlStatementStat stat = config.getSqlStatementStat();
        if (stat != null) {
          long end = System.currentTimeMillis();
          long elapsed = end - start;
          stat.save(config.name, "query", sql, paras, result.size(), start, elapsed, config.writeSync);
        }
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, paras, e);
      }
      return result;
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
  }

  public <T> List<T> query(String sql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return query(config, conn, sql, paras);
    } finally {
      config.close(conn);
    }
  }

  public List<byte[]> queryListBytes(String sql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return queryListBytes(config, conn, sql, paras);
    } finally {
      config.close(conn);
    }
  }

  /**
   * @param sql an SQL statement
   * @see #query(String, Object...)
   */
  public <T> List<T> query(String sql) { // return List<object[]> or List<object>
    return query(sql, DbKit.NULL_PARA_ARRAY);
  }

  public <T> List<T> query(SqlPara sqlPara) {
    return query(sqlPara.getSql(), sqlPara.getPara());
  }

  /**
   * Execute sql query and return the first result. I recommend add "limit 1" in
   * your sql.
   *
   * @param sql   an SQL statement that may contain one or more '?' IN parameter
   *              placeholders
   * @param paras the parameters of sql
   * @return Object[] if your sql has select more than one column, and it return
   *         Object if your sql has select only one column.
   */
  public <T> T queryFirst(String sql, Object... paras) {
    List<T> result = query(sql, paras);
    return (result.size() > 0 ? result.get(0) : null);
  }

  /**
   * @param sql an SQL statement
   * @see #queryFirst(String, Object...)
   */
  public <T> T queryFirst(String sql) {
    // return queryFirst(sql, NULL_PARA_ARRAY);
    List<T> result = query(sql, DbKit.NULL_PARA_ARRAY);
    return (result.size() > 0 ? result.get(0) : null);
  }

  // 26 queryXxx method below -----------------------------------------------

  /**
   * Execute sql query just return one column.
   *
   * @param <T>   the type of the column that in your sql's select statement
   * @param sql   an SQL statement that may contain one or more '?' IN parameter
   *              placeholders
   * @param paras the parameters of sql
   * @return <T> T
   */
  public <T> T queryColumn(String sql, Object... paras) {
    List<T> result = query(sql, paras);
    if (result.size() > 0) {
      T temp = result.get(0);
      if (temp instanceof Object[]) {
        throw new ActiveRecordException("Only ONE COLUMN can be queried.");
      }
      return temp;
    }
    return null;
  }

  public <T> T queryColumnById(String tableName, String column, Object id) {
    return queryColumnByField(tableName, column, "id", id);
  }

  public <T> T queryColumnByField(String tableName, String column, String field, Object value) {
    String sql = String.format(queryColumnByField, column, tableName, field);
    return queryColumn(sql, value);
  }

  public String queryStr(String sql, Object... paras) {
    Object s = queryColumn(sql, paras);
    return s != null ? s.toString() : null;
  }

  public String queryStr(String sql) {
    return queryStr(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Integer queryInt(String sql, Object... paras) {
    Number n = queryNumber(sql, paras);
    return n != null ? n.intValue() : null;
  }

  public Integer queryInt(String sql) {
    return queryInt(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Long queryLong(String sql, Object... paras) {
    Number n = queryNumber(sql, paras);
    return n != null ? n.longValue() : null;
  }

  public Long queryLong(String sql) {
    return queryLong(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Double queryDouble(String sql, Object... paras) {
    Number n = queryNumber(sql, paras);
    return n != null ? n.doubleValue() : null;
  }

  public Double queryDouble(String sql) {
    return queryDouble(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Float queryFloat(String sql, Object... paras) {
    Number n = queryNumber(sql, paras);
    return n != null ? n.floatValue() : null;
  }

  public Float queryFloat(String sql) {
    return queryFloat(sql, DbKit.NULL_PARA_ARRAY);
  }

  public BigDecimal queryBigDecimal(String sql, Object... paras) {
    Object n = queryColumn(sql, paras);
    if (n instanceof BigDecimal) {
      return (BigDecimal) n;
    } else if (n != null) {
      return new BigDecimal(n.toString());
    } else {
      return null;
    }
  }

  public BigDecimal queryBigDecimal(String sql) {
    return queryBigDecimal(sql, DbKit.NULL_PARA_ARRAY);
  }

  public BigInteger queryBigInteger(String sql, Object... paras) {
    Object n = queryColumn(sql, paras);
    if (n instanceof BigInteger) {
      return (BigInteger) n;
    } else if (n != null) {
      return new BigInteger(n.toString());
    } else {
      return null;
    }
  }

  public BigInteger queryBigInteger(String sql) {
    return queryBigInteger(sql, DbKit.NULL_PARA_ARRAY);
  }

  public byte[] queryBytes(String sql, Object... paras) {
    List<byte[]> result = queryListBytes(sql, paras);
    return (result.size() > 0 ? result.get(0) : null);
  }

  public byte[] queryBytes(String sql) {
    return (byte[]) queryColumn(sql, DbKit.NULL_PARA_ARRAY);
  }

  public java.util.Date queryDate(String sql, Object... paras) {
    Object d = queryColumn(sql, paras);

    if (d instanceof Temporal) {
      if (d instanceof LocalDateTime) {
        return TimeKit.toDate((LocalDateTime) d);
      }
      if (d instanceof LocalDate) {
        return TimeKit.toDate((LocalDate) d);
      }
      if (d instanceof LocalTime) {
        return TimeKit.toDate((LocalTime) d);
      }
    }

    return (java.util.Date) d;
  }

  public java.util.Date queryDate(String sql) {
    return queryDate(sql, DbKit.NULL_PARA_ARRAY);
  }

  public LocalDateTime queryLocalDateTime(String sql, Object... paras) {
    Object d = queryColumn(sql, paras);

    if (d instanceof LocalDateTime) {
      return (LocalDateTime) d;
    }
    if (d instanceof LocalDate) {
      return ((LocalDate) d).atStartOfDay();
    }
    if (d instanceof LocalTime) {
      return LocalDateTime.of(LocalDate.now(), (LocalTime) d);
    }
    if (d instanceof java.util.Date) {
      return TimeKit.toLocalDateTime((java.util.Date) d);
    }

    return (LocalDateTime) d;
  }

  public LocalDateTime queryLocalDateTime(String sql) {
    return queryLocalDateTime(sql, DbKit.NULL_PARA_ARRAY);
  }

  public java.sql.Time queryTime(String sql, Object... paras) {
    return (java.sql.Time) queryColumn(sql, paras);
  }

  public java.sql.Time queryTime(String sql) {
    return (java.sql.Time) queryColumn(sql, DbKit.NULL_PARA_ARRAY);
  }

  public java.sql.Timestamp queryTimestamp(String sql, Object... paras) {
    return (java.sql.Timestamp) queryColumn(sql, paras);
  }

  public java.sql.Timestamp queryTimestamp(String sql) {
    return (java.sql.Timestamp) queryColumn(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Boolean queryBoolean(String sql, Object... paras) {
    return (Boolean) queryColumn(sql, paras);
  }

  public Boolean queryBoolean(String sql) {
    return (Boolean) queryColumn(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Short queryShort(String sql, Object... paras) {
    Number n = queryNumber(sql, paras);
    return n != null ? n.shortValue() : null;
  }

  public Short queryShort(String sql) {
    return queryShort(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Byte queryByte(String sql, Object... paras) {
    Number n = queryNumber(sql, paras);
    return n != null ? n.byteValue() : null;
  }

  public Byte queryByte(String sql) {
    return queryByte(sql, DbKit.NULL_PARA_ARRAY);
  }

  public Number queryNumber(String sql, Object... paras) {
    return (Number) queryColumn(sql, paras);
  }

  public Number queryNumber(String sql) {
    return (Number) queryColumn(sql, DbKit.NULL_PARA_ARRAY);
  }

  public PGobject queryPGobject(String sql, Object... paras) {
    return queryColumn(sql, paras);
  }
  // 26 queryXxx method under -----------------------------------------------

  /**
   * Execute sql update
   */
  public int update(Config config, Connection conn, String sql, Object... paras) {
    PreparedStatement pst;
    try {
      pst = conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    try {
      config.dialect.fillStatement(pst, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }

    long start = System.currentTimeMillis();
    int result;
    try {
      result = pst.executeUpdate();
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    } finally {
      if (pst != null) {
        try {
          pst.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras, e);
        }
      }
    }
    ISqlStatementStat stat = config.getSqlStatementStat();
    if (stat != null) {
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      stat.save(config.name, "update", sql, paras, result, start, elapsed, config.writeSync);
    }
    return result;

  }

  /**
   * Execute update, insert or delete sql statement.
   *
   * @param sql   an SQL statement that may contain one or more '?' IN parameter
   *              placeholders
   * @param paras the parameters of sql
   * @return either the row count for <code>INSERT</code>, <code>UPDATE</code>, or
   *         <code>DELETE</code> statements, or 0 for SQL statements that return
   *         nothing
   */
  public int update(String sql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return update(config, conn, sql, paras);
    } finally {
      config.close(conn);
    }
  }

  /**
   * @param sql an SQL statement
   * @see #update(String, Object...)
   */
  public int update(String sql) {
    return update(sql, DbKit.NULL_PARA_ARRAY);
  }

  public List<Row> findJsonField(Config config, Connection conn, String sql, String[] jsonFields, Object... paras) {
    try (PreparedStatement pst = conn.prepareStatement(sql)) {
      config.dialect.fillStatement(pst, paras);
      List<Row> result = null;
      long start = System.currentTimeMillis();
      try (ResultSet rs = pst.executeQuery()) {
        result = config.dialect.buildRecordListWithJsonFields(config, rs, jsonFields); // RecordBuilder.build(config, rs);
        ISqlStatementStat stat = config.getSqlStatementStat();
        if (stat != null) {
          long end = System.currentTimeMillis();
          long elapsed = end - start;
          stat.save(config.name, "save", sql, paras, result.size(), start, elapsed, config.writeSync);
        }
      }
      return result;
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
  }

  public List<Row> find(Config config, Connection conn, String sql, Object... paras) {
    PreparedStatement pst;
    try {
      pst = conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    try {
      config.dialect.fillStatement(pst, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }

    List<Row> result = null;
    ResultSet rs;
    long start = System.currentTimeMillis();
    try {
      rs = pst.executeQuery();
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    try {
      result = config.dialect.buildRecordList(config, rs);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras, e);
        }
      }
      if (pst != null) {
        try {
          pst.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras, e);
        }
      }
    }

    ISqlStatementStat stat = config.getSqlStatementStat();
    if (stat != null) {
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      stat.save(config.name, "find", sql, paras, result.size(), start, elapsed, config.writeSync);
    }
    return result;

  }

  public List<Row> find(Config config, Connection conn, String tableName, String columns, Row record) {
    List<Object> paras = new ArrayList<>();

    StringBuffer sqlBuffer = config.dialect.forDbFind(tableName, columns, record, paras);
    PreparedStatement pst;
    String sql = sqlBuffer.toString();
    try {
      pst = conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }
    try {
      config.dialect.fillStatement(pst, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }

    List<Row> result = null;
    ResultSet rs;
    long start = System.currentTimeMillis();
    try {
      rs = pst.executeQuery();
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }
    try {
      result = config.dialect.buildRecordList(config, rs);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
        }
      }
      if (pst != null) {
        try {
          pst.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
        }
      }
    }

    ISqlStatementStat stat = config.getSqlStatementStat();
    if (stat != null) {
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      stat.save(config.name, "find", sql, paras, result.size(), start, elapsed, config.writeSync);
    }
    return result;
  }

  public List<Row> findByField(Config config, Connection conn, String tableName, String columns, String field, Object fieldValue) {

    List<Object> paras = new ArrayList<>();

    StringBuffer sqlBuffer = config.dialect.forDbFindByField(tableName, columns, field, fieldValue, paras);
    PreparedStatement pst;
    String sql = sqlBuffer.toString();
    try {
      pst = conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }
    try {
      config.dialect.fillStatement(pst, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }

    List<Row> result = null;
    ResultSet rs;
    long start = System.currentTimeMillis();
    try {
      rs = pst.executeQuery();
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }
    try {
      result = config.dialect.buildRecordList(config, rs);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
        }
      }
      if (pst != null) {
        try {
          pst.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
        }
      }
    }

    ISqlStatementStat stat = config.getSqlStatementStat();
    if (stat != null) {
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      stat.save(config.name, "find", sql, paras, result.size(), start, elapsed, config.writeSync);
    }
    return result;
  }

  public List<Row> find(Config config, Connection conn, String sql, List paras) {
    try (PreparedStatement pst = conn.prepareStatement(sql)) {
      config.dialect.fillStatement(pst, paras);
      List<Row> result = null;
      long start = System.currentTimeMillis();
      try (ResultSet rs = pst.executeQuery()) {
        result = config.dialect.buildRecordList(config, rs); // RecordBuilder.build(config, rs);
        ISqlStatementStat stat = config.getSqlStatementStat();
        if (stat != null) {
          long end = System.currentTimeMillis();
          long elapsed = end - start;
          stat.save(config.name, "find", sql, paras, result.size(), start, elapsed, config.writeSync);
        }
      }
      return result;
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }
  }

  public <T> List<T> find(Class<T> clazz, Config config, Connection conn, String sql, Object... paras) {
    List<Row> result = null;
    try (PreparedStatement pst = conn.prepareStatement(sql)) {
      config.dialect.fillStatement(pst, paras);
      long start = System.currentTimeMillis();
      try (ResultSet rs = pst.executeQuery()) {
        result = config.dialect.buildRecordList(config, rs); // RecordBuilder.build(config, rs);
        ISqlStatementStat stat = config.getSqlStatementStat();
        if (stat != null) {
          long end = System.currentTimeMillis();
          long elapsed = end - start;
          stat.save(config.name, "find", sql, paras, result.size(), start, elapsed, config.writeSync);
        }
      }
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }

    List<T> collect = new ArrayList<>(result.size());
    for (Row e : result) {
      collect.add(e.toBean(clazz));
    }
    return collect;
  }

  /**
   * @see #findWithPrimaryKey(String, String, Object...)
   */
  public List<Row> find(String sql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return find(config, conn, sql, paras);
    } finally {
      config.close(conn);
    }
  }

  public List<Row> find(String tableName, Row record) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return find(config, conn, tableName, "*", record);
    } finally {
      config.close(conn);
    }
  }

  public List<Row> findByField(String tableName, String field, Object fieldValue) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return findByField(config, conn, tableName, "*", field, fieldValue);
    } finally {
      config.close(conn);
    }
  }

  public List<Row> find(String tableName, String columns, Row record) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return find(config, conn, tableName, columns, record);
    } finally {
      config.close(conn);
    }
  }

  public List<Row> find(String sql, List paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return find(config, conn, sql, paras);
    } finally {
      config.close(conn);
    }
  }

  public List<Row> findJsonField(String sql, String[] jsonFields, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return findJsonField(config, conn, sql, jsonFields, paras);
    } finally {
      config.close(conn);
    }
  }

  public List<Row> findWithJsonField(String sql, String[] jsonFields, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return findJsonField(config, conn, sql, jsonFields, paras);
    } finally {
      config.close(conn);
    }
  }

  public <T> List<T> find(Class<T> clazz, String sql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return find(clazz, config, conn, sql, paras);
    } finally {
      config.close(conn);
    }
  }

  /**
   * @param sql the sql statement
   * @see #findWithPrimaryKey(String, String, Object...)
   */
  public List<Row> find(String sql) {
    return find(sql, DbKit.NULL_PARA_ARRAY);
  }

  public List<Row> findWithJsonFields(String sql, String[] jsonFields) {
    return findWithJsonField(sql, jsonFields, DbKit.NULL_PARA_ARRAY);
  }

  public List<Row> findAll(String tableName) {
    String sql = config.dialect.forFindAll(tableName);
    return find(sql, DbKit.NULL_PARA_ARRAY);
  }

  public <T> List<T> findAll(Class<T> clazz, String tableName) {
    String sql = config.dialect.forFindAll(tableName);
    return find(clazz, sql, DbKit.NULL_PARA_ARRAY);
  }

  public List<Row> findIn(String tableName, String primayKey, Object... paras) {

    StringBuilder ids = new StringBuilder();
    for (int i = 0; i < paras.length; i++) {
      ids.append("?");
      if (i < paras.length - 1) {
        ids.append(", ");
      }
    }
    String sql = String.format("SELECT * FROM %s WHERE " + primayKey + " IN (" + ids.toString() + ")", tableName);
    return find(sql, paras);
  }

  public List<Row> findColumnsIn(String tableName, String columns, String primayKey, Object... paras) {
    StringBuilder ids = new StringBuilder();
    for (int i = 0; i < paras.length; i++) {
      ids.append("?");
      if (i < paras.length - 1) {
        ids.append(", ");
      }
    }

    String sql = config.dialect.forDbFindColumns(tableName, columns);
    sql = sql + " WHERE " + primayKey + " IN (" + ids.toString() + ")";
    return find(sql, paras);
  }

  public List<Row> findColumnsIn(String tableName, String columns, String primayKey, List paras) {
    StringBuilder ids = new StringBuilder();
    for (int i = 0; i < paras.size(); i++) {
      ids.append("?");
      if (i < paras.size() - 1) {
        ids.append(", ");
      }
    }

    String sql = config.dialect.forDbFindColumns(tableName, columns);
    sql = sql + " WHERE " + primayKey + " IN (" + ids.toString() + ")";
    return find(sql, paras);
  }

  public List<Row> findColumnsAll(String tableName, String columns) {
    String sql = config.dialect.forDbFindColumns(tableName, columns);
    return find(sql, DbKit.NULL_PARA_ARRAY);
  }

  public <T> List<T> findColumnsAll(Class<T> clazz, String tableName, String columns) {
    String sql = config.dialect.forDbFindColumns(tableName, columns);
    return find(clazz, sql, DbKit.NULL_PARA_ARRAY);
  }

  public List<Row> findByColumn(String tableName, String column, String value) {
    String sql = config.dialect.forDbFindById(tableName, new String[] { column });
    return find(sql, value);
  }

  /**
   * Find first record. I recommend add "limit 1" in your sql.
   *
   * @param sql   an SQL statement that may contain one or more '?' IN parameter
   *              placeholders
   * @param paras the parameters of sql
   * @return the Record object
   */
  public Row findFirst(String sql, Object... paras) {
    List<Row> result = find(sql, paras);
    return result.size() > 0 ? result.get(0) : null;
  }

  public Row findFirst(String tableName, Row record) {
    List<Row> result = find(tableName, "*", record);
    return result.size() > 0 ? result.get(0) : null;
  }

  public Row findFirst(String tableName, String columns, Row record) {
    List<Row> result = find(tableName, columns, record);
    return result.size() > 0 ? result.get(0) : null;
  }

  public Row findFirstJsonField(String sql, String[] jsonFields, Object... paras) {
    // List<Record> result = find(sql, jsonFields, paras);
    List<Row> result = findJsonField(sql, jsonFields, paras);
    return result.size() > 0 ? result.get(0) : null;
  }

  public <T> T findFirst(Class<T> clazz, String sql, Object... paras) {
    List<T> result = find(clazz, sql, paras);
    return result.size() > 0 ? result.get(0) : null;
  }

  /**
   * @param sql an SQL statement
   * @see #findFirst(String, Object...)
   */
  public Row findFirst(String sql) {
    return findFirst(sql, DbKit.NULL_PARA_ARRAY);
  }

  /**
   * Find record by id with default primary key.
   * 
   * <pre>
   * Example:
   * Record user = Db.use().findById("user", 15);
   * </pre>
   *
   * @param tableName the table name of the table
   * @param idValue   the id value of the record
   */
  public Row findById(String tableName, Object idValue) {
    return findByIds(tableName, config.dialect.getDefaultPrimaryKey(), idValue);
  }

  public <T> T findById(Class<T> clazz, String tableName, Object idValue) {
    return findByIds(clazz, tableName, config.dialect.getDefaultPrimaryKey(), idValue);
  }

  public Row findColumnsById(String tableName, String columns, Object idValue) {
    return findColumnsById(tableName, columns, config.dialect.getDefaultPrimaryKey(), idValue);
  }

  public <T> T findColumnsById(Class<T> clazz, String tableName, String columns, Object idValue) {
    return findColumnsById(clazz, tableName, columns, config.dialect.getDefaultPrimaryKey(), idValue);
  }

  public Row findById(String tableName, String primaryKey, Object idValue) {
    return findByIds(tableName, primaryKey, idValue);
  }

  public <T> T findById(Class<T> clazz, String tableName, String primaryKey, Object idValue) {
    return findByIds(clazz, tableName, primaryKey, idValue);
  }

  /**
   * Find record by ids.
   * 
   * <pre>
   * Example:
   * Record user = Db.use().findByIds("user", "user_id", 123);
   * Record userRole = Db.use().findByIds("user_role", "user_id, role_id", 123, 456);
   * </pre>
   *
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param idValues   the id value of the record, it can be composite id values
   */
  public Row findByIds(String tableName, String primaryKey, Object... idValues) {
    List<Row> result = findWithPrimaryKey(tableName, primaryKey, idValues);
    return result.size() > 0 ? result.get(0) : null;
  }

  public <T> T findByIds(Class<T> clazz, String tableName, String primaryKey, Object... idValues) {
    List<Row> result = findWithPrimaryKey(tableName, primaryKey, idValues);
    List<T> collect = result.stream().map((e) -> e.toBean(clazz)).collect(Collectors.toList());
    return result.size() > 0 ? collect.get(0) : null;
  }

  public Row findColumnsByIds(String tableName, String columns, String primaryKey, Object... idValues) {
    List<Row> result = findColumns(tableName, columns, primaryKey, idValues);
    return result.size() > 0 ? result.get(0) : null;
  }

  public <T> T findColumnsByIds(Class<T> clazz, String tableName, String columns, String primaryKey, Object... idValues) {
    List<Row> result = findColumns(tableName, columns, primaryKey, idValues);
    List<T> collect = result.stream().map((e) -> e.toBean(clazz)).collect(Collectors.toList());
    return result.size() > 0 ? collect.get(0) : null;
  }

  public List<Row> findWithPrimaryKey(String tableName, String primaryKey, Object... idValues) {
    String[] pKeys = primaryKey.split(",");
    if (pKeys.length != idValues.length) {
      throw new IllegalArgumentException("primary key number must equals id value number");
    }

    String sql = config.dialect.forDbFindById(tableName, pKeys);
    List<Row> result = find(sql, idValues);
    return result;
  }

  public Row findColumnsById(String tableName, String columns, String primaryKey, Object... idValues) {
    List<Row> result = findColumns(tableName, columns, primaryKey, idValues);
    return result.size() > 0 ? result.get(0) : null;
  }

  public <T> T findColumnsById(Class<T> clazz, String tableName, String columns, String primaryKey, Object... idValues) {
    List<Row> result = findColumns(tableName, columns, primaryKey, idValues);
    List<T> collect = result.stream().map((e) -> e.toBean(clazz)).collect(Collectors.toList());
    return result.size() > 0 ? collect.get(0) : null;
  }

  public List<Row> findColumns(String tableName, String columns, String primaryKey, Object... idValues) {
    String[] pKeys = primaryKey.split(",");
    if (pKeys.length != idValues.length) {
      throw new IllegalArgumentException("primary key number must equals id value number");
    }

    String sql = config.dialect.forDbFindColumnsById(tableName, columns, pKeys);
    List<Row> result = find(sql, idValues);
    return result;
  }

  /**
   * Delete record by id with default primary key.
   * 
   * <pre>
   * Example: Db.use().deleteById("user", 15);
   * </pre>
   *
   * @param tableName the table name of the table
   * @param idValue   the id value of the record
   * @return true if delete succeed otherwise false
   */
  public boolean deleteById(String tableName, Object idValue) {
    return deleteByIds(tableName, config.dialect.getDefaultPrimaryKey(), idValue);
  }

  public boolean deleteById(String tableName, String primaryKey, Object idValue) {
    return deleteByIds(tableName, primaryKey, idValue);
  }

  /**
   * Delete record by ids.
   * 
   * <pre>
   * Example: Db.use().deleteByIds("user", "user_id", 15);
   * Db.use().deleteByIds("user_role", "user_id, role_id", 123, 456);
   * </pre>
   *
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param idValues   the id value of the record, it can be composite id values
   * @return true if delete succeed otherwise false
   */
  public boolean deleteByIds(String tableName, String primaryKey, Object... idValues) {
    String[] pKeys = primaryKey.split(",");
    if (pKeys.length != idValues.length)
      throw new IllegalArgumentException("primary key number must equals id value number");

    String sql = config.dialect.forDbDeleteById(tableName, pKeys);
    return update(sql, idValues) >= 1;
  }

  /**
   * Delete record.
   * 
   * <pre>
   * Example:
   * boolean succeed = Db.use().delete("user", "id", user);
   * </pre>
   *
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param record     the record
   * @return true if delete succeed otherwise false
   */
  public boolean delete(String tableName, String primaryKey, Row record) {
    String[] pKeys = primaryKey.split(",");
    if (pKeys.length <= 1) {
      Object t = record.get(primaryKey); // 引入中间变量避免 JDK 8 传参有误
      return deleteByIds(tableName, primaryKey, t);
    }

    config.dialect.trimPrimaryKeys(pKeys);
    Object[] idValue = new Object[pKeys.length];
    for (int i = 0; i < pKeys.length; i++) {
      idValue[i] = record.get(pKeys[i]);
      if (idValue[i] == null)
        throw new IllegalArgumentException("The value of primary key \"" + pKeys[i] + "\" can not be null in record object");
    }
    return deleteByIds(tableName, primaryKey, idValue);
  }

  /**
   * <pre>
   * Example:
   * boolean succeed = Db.use().delete("user", user);
   * </pre>
   *
   * @see #delete(String, String, Row)
   */
  public boolean deleteByIds(String tableName, Row record) {
    String defaultPrimaryKey = config.dialect.getDefaultPrimaryKey();
    Object t = record.get(defaultPrimaryKey); // 引入中间变量避免 JDK 8 传参有误
    return deleteByIds(tableName, defaultPrimaryKey, t);
  }

  /**
   * <pre>
   * Example:
   * String noteId="0000000";
   * Record removeRecordFilter = new Record();
   * removeRecordFilter.set("note_id", noteId);
   * Db.delete(ENoteTableNames.ENOTE_NOTE_TAG, removeRecordFilter);
   * </pre>
   */
  public boolean delete(String tableName, Row record) {
    // 判断record是否为空或没有字段
    if (record == null) {
      return false;
    }

    Map<String, Object> columns = record.getColumns();
    if (columns.size() < 1) {
      return false;
    }

    StringBuilder sql = new StringBuilder("DELETE FROM ");
    sql.append(tableName);
    sql.append(" WHERE ");

    List<Object> paras = new ArrayList<>();
    boolean isFirst = true;

    // 遍历record中的所有字段，构建SQL语句和参数列表
    for (Map.Entry<String, Object> entry : columns.entrySet()) {
      if (!isFirst) {
        sql.append(" AND ");
      } else {
        isFirst = false;
      }

      sql.append(entry.getKey());
      sql.append(" = ?");
      paras.add(entry.getValue());
    }

    // 调用下面的delete方法执行SQL
    int result = delete(sql.toString(), paras.toArray());

    // 如果受影响的行数大于0，则返回true，表示删除成功
    return result > 0;
  }

  /**
   * Execute delete sql statement.
   *
   * @param sql   an SQL statement that may contain one or more '?' IN parameter
   *              placeholders
   * @param paras the parameters of sql
   * @return the row count for <code>DELETE</code> statements, or 0 for SQL
   *         statements that return nothing
   */
  public int delete(String sql, Object... paras) {
    return update(sql, paras);
  }

  /**
   * @param sql an SQL statement
   * @see #delete(String, Object...)
   */
  public int delete(String sql) {
    return update(sql);
  }

  private <T> Page<T> countPage(Config config, Connection conn, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, StringBuilder findSql, Object... paras) throws SQLException {
    if (pageNumber < 1 || pageSize < 1) {
      throw new ActiveRecordException("pageNumber and pageSize must more than 0");
    }
    if (config.dialect.isTakeOverDbPaginate()) {
      return config.dialect.takeOverDbPaginate(conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    }

    List result = query(config, conn, totalRowSql, paras);
    int size = result.size();
    if (isGroupBySql == null) {
      isGroupBySql = size > 1;
    }

    long totalRow;
    if (isGroupBySql) {
      totalRow = size;
    } else {
      totalRow = (size > 0) ? ((Number) result.get(0)).longValue() : 0;
    }
    if (totalRow == 0) {
      return new Page<T>(new ArrayList<T>(0), pageNumber, pageSize, 0, 0);
    }

    int totalPage = (int) (totalRow / pageSize);
    if (totalRow % pageSize != 0) {
      totalPage++;
    }

    if (pageNumber > totalPage) {
      return new Page<T>(new ArrayList<T>(0), pageNumber, pageSize, totalPage, (int) totalRow);
    }
    Page<T> page = new Page<T>(pageNumber, pageSize, totalPage, (int) totalRow);
    return page;
  }

  /**
   * Paginate.
   *
   * @param pageNumber      the page number
   * @param pageSize        the page size
   * @param select          the select part of the sql statement
   * @param sqlExceptSelect the sql statement excluded select part
   * @param paras           the parameters of sql
   * @return the Page object
   */
  public Page<Row> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) {
    return doPaginate(pageNumber, pageSize, null, select, sqlExceptSelect, paras);
  }

  public <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object[] paras) {
    return doPaginate(clazz, pageNumber, pageSize, null, select, sqlExceptSelect, paras);
  }

  public Page<Row> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect) {
    return doPaginate(pageNumber, pageSize, null, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  public Page<Row> paginateJsonFields(int pageNumber, int pageSize, String select, String sqlExceptSelect, String[] jsonFields, Object... paras) {
    return doPaginateJsonFields(pageNumber, pageSize, null, select, sqlExceptSelect, jsonFields, paras);
  }

  public Page<Row> paginateJsonFields(int pageNumber, int pageSize, String select, String sqlExceptSelect, String[] jsonFields) {
    return doPaginateJsonFields(pageNumber, pageSize, null, select, sqlExceptSelect, jsonFields, DbKit.NULL_PARA_ARRAY);
  }

  public Page<Row> paginate(int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect) {
    return doPaginate(pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  public <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, String select, String sqlExceptSelect) {
    return doPaginate(clazz, pageNumber, pageSize, null, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  public <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect) {
    return doPaginate(clazz, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  public Page<Row> paginate(int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    return doPaginate(pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
  }

  public <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect, Object[] paras) {
    return doPaginate(clazz, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
  }

  public Page<Row> doPaginateJsonFields(int pageNumber, int pageSize, Boolean isGroupBySql, String select, String sqlExceptSelect, String[] jsonFields, Object... paras) {

    Connection conn = null;
    try {
      conn = config.getConnection();
      String totalRowSql = config.dialect.forPaginateTotalRow(select, sqlExceptSelect, null);
      StringBuilder findSql = new StringBuilder();
      findSql.append(select).append(' ').append(sqlExceptSelect);
      return doPaginateByFullSqlWithJsonFields(config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, jsonFields, paras);
    } finally {
      config.close(conn);
    }
  }

  public Page<Row> doPaginate(int pageNumber, int pageSize, Boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      String totalRowSql = config.dialect.forPaginateTotalRow(select, sqlExceptSelect, null);
      StringBuilder findSql = new StringBuilder();
      findSql.append(select).append(' ').append(sqlExceptSelect);
      return doPaginateByFullSql(config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    } finally {
      config.close(conn);
    }
  }

  public <T> Page<T> doPaginate(Class<T> clazz, int pageNumber, int pageSize, Boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      String totalRowSql = config.dialect.forPaginateTotalRow(select, sqlExceptSelect, null);
      StringBuilder findSql = new StringBuilder();
      findSql.append(select).append(' ').append(sqlExceptSelect);
      return doPaginateByFullSql(clazz, config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    } finally {
      config.close(conn);
    }
  }

  public Page<Row> doPaginateByFullSqlWithJsonFields(Config config, Connection conn, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, StringBuilder findSql, String[] jsonFields,
      Object... paras) {
    String sql = config.dialect.forPaginate(pageNumber, pageSize, findSql);

    Page<Row> page = null;
    try {
      page = countPage(config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }
    List<Row> list = null;
    list = findJsonField(config, conn, sql, jsonFields, paras);
    page.setList(list);
    return page;
  }

  public Page<Row> doPaginateByFullSql(Config config, Connection conn, int pageNumber, int pageSize,
      //
      Boolean isGroupBySql, String totalRowSql, StringBuilder findSql, Object... paras) {
    Page<Row> page = null;
    try {
      page = countPage(config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), findSql.toString(), paras, e);
    }

    String sql = config.dialect.forPaginate(pageNumber, pageSize, findSql);
    List<Row> list = find(config, conn, sql, paras);
    page.setList(list);
    return page;
  }

  public <T> Page<T> doPaginateByFullSql(Class<T> clazz, Config config2, Connection conn, int pageNumber,
      //
      int pageSize, Boolean isGroupBySql, String totalRowSql, StringBuilder findSql, Object[] paras) {
    Page<T> page;
    try {
      page = countPage(config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), findSql.toString(), paras, e);
    }
    // find with sql
    String sql = config.dialect.forPaginate(pageNumber, pageSize, findSql);
    List<T> list = find(clazz, config, conn, sql, paras);
    page.setList(list);
    return page;
  }

  public Page<Row> paginate(Config config, Connection conn, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) throws SQLException {
    String totalRowSql = config.dialect.forPaginateTotalRow(select, sqlExceptSelect, null);
    StringBuilder findSql = new StringBuilder();
    findSql.append(select).append(' ').append(sqlExceptSelect);
    return doPaginateByFullSql(config, conn, pageNumber, pageSize, null, totalRowSql, findSql, paras);
  }

  public Page<Row> doPaginateByFullSql(int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      StringBuilder findSqlBuf = new StringBuilder().append(findSql);
      return doPaginateByFullSql(config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSqlBuf, paras);
    } finally {
      config.close(conn);
    }
  }

  public <T> Page<T> doPaginateByFullSql(Class<T> clazz, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      StringBuilder findSqlBuf = new StringBuilder().append(findSql);
      return doPaginateByFullSql(clazz, config, conn, pageNumber, pageSize, isGroupBySql, totalRowSql, findSqlBuf, paras);
    } finally {
      config.close(conn);
    }
  }

  public Page<Row> paginateByFullSql(int pageNumber, int pageSize, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByFullSql(pageNumber, pageSize, null, totalRowSql, findSql, paras);
  }

  public Page<Row> paginateByFullSql(int pageNumber, int pageSize, boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByFullSql(pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
  }

  public <T> Page<T> paginateByFullSql(Class<T> clazz, int pageNumber, int pageSize, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByFullSql(clazz, pageNumber, pageSize, null, totalRowSql, findSql, paras);
  }

  public <T> Page<T> paginateByFullSql(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByFullSql(clazz, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
  }

  public boolean save(Config config, Connection conn, String sql, Object... paras) {
    PreparedStatement pst = null;
    if (config.dialect.isOracle()) {
      try {
        pst = conn.prepareStatement(sql);
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, paras, e);
      }
    } else {
      try {
        pst = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, paras, e);
      }
    }

    try {
      config.dialect.fillStatement(pst, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    }

    long start = System.currentTimeMillis();
    int result = 0;
    try {
      result = pst.executeUpdate();
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras, e);
    } finally {
      if (pst != null) {
        try {
          pst.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras, e);
        }
      }
    }

    ISqlStatementStat stat = config.getSqlStatementStat();
    if (stat != null) {
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      stat.save(config.name, "save", sql, paras, result, start, elapsed, config.writeSync);
    }
    return result >= 1;
  }

  public boolean save(Config config, Connection conn, String tableName, String primaryKey, Row record) {
    String[] pKeys = primaryKey.split(",");
    List<Object> paras = new ArrayList<Object>();
    StringBuilder sql = new StringBuilder();
    config.dialect.forDbSave(tableName, pKeys, record, sql, paras);

    String sqlString = sql.toString();
    return save(config, conn, sqlString, pKeys, paras, record);
  }

  public boolean saveIfAbset(Config config, Connection conn, String tableName, String primaryKey, Row record) {
    String[] pKeys = primaryKey.split(",");
    List<Object> paras = new ArrayList<Object>();
    StringBuilder sql = new StringBuilder();
    config.dialect.forDbSaveIfAbset(tableName, pKeys, record, sql, paras);
    String executedSql = sql.toString();
    return save(config, conn, executedSql, pKeys, paras, record);
  }

  private boolean save(Config config, Connection conn, String sql, String[] pKeys, List<Object> paras, Row record) {
    PreparedStatement pst = null;
    if (config.dialect.isOracle()) {
      try {
        pst = conn.prepareStatement(sql, pKeys);
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
      }
    } else {
      try {
        pst = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
      }
    }

    try {
      config.dialect.fillStatement(pst, paras);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    }

    long start = System.currentTimeMillis();
    int result = 0;
    try {
      result = pst.executeUpdate();
      config.dialect.getRecordGeneratedKey(pst, record, pKeys);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
    } finally {
      if (pst != null) {
        try {
          pst.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, paras.toArray(), e);
        }
      }
    }

    ISqlStatementStat stat = config.getSqlStatementStat();
    if (stat != null) {
      long end = System.currentTimeMillis();
      long elapsed = end - start;
      stat.save(config.name, "save", sql, paras.toArray(), result, start, elapsed, config.writeSync);
    }
    record.clearModifyFlag();
    return result >= 1;
  }

  public boolean save(Config config, Connection conn, String tableName, String primaryKey, Row record, String[] jsonFields) {
    String[] pKeys = primaryKey.split(",");
    List<Object> paras = new ArrayList<Object>();
    StringBuilder sql = new StringBuilder();
    config.dialect.transformJsonFields(record, jsonFields);
    config.dialect.forDbSave(tableName, pKeys, record, sql, paras);
    int result = 0;
    String sqlString = sql.toString();
    try (PreparedStatement pst = config.dialect.isOracle() ? conn.prepareStatement(sqlString, pKeys) : conn.prepareStatement(sqlString, Statement.RETURN_GENERATED_KEYS)) {
      config.dialect.fillStatement(pst, paras);
      long start = System.currentTimeMillis();
      result = pst.executeUpdate();
      ISqlStatementStat stat = config.getSqlStatementStat();
      if (stat != null) {
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        stat.save(config.name, "save", sqlString, paras.toArray(), result, start, elapsed, config.writeSync);
      }
      config.dialect.getRecordGeneratedKey(pst, record, pKeys);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sqlString, paras.toArray(), e);
    }
    record.clearModifyFlag();
    return result >= 1;
  }

  /**
   * Save record.
   * 
   * <pre>
   * Example:
   * Record userRole = new Record().set("user_id", 123).set("role_id", 456);
   * Db.use().save("user_role", "user_id, role_id", userRole);
   * </pre>
   *
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param record     the record will be saved
   */
  public boolean save(String tableName, String primaryKey, Row record) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return save(config, conn, tableName, primaryKey, record);
    } finally {
      config.close(conn);
    }
  }

  public boolean saveIfAbset(String tableName, String primaryKey, Row record) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return saveIfAbset(config, conn, tableName, primaryKey, record);
    } finally {
      config.close(conn);
    }
  }

  public boolean save(String sql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return save(config, conn, sql, paras);
    } finally {
      config.close(conn);
    }
  }

  public boolean save(String tableName, String primaryKey, Row record, String[] jsonFields) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return save(config, conn, tableName, primaryKey, record, jsonFields);
    } finally {
      config.close(conn);
    }
  }

  /**
   * @see #save(String, String, Row)
   */
  public boolean save(String tableName, Row record) {
    return save(tableName, config.dialect.getDefaultPrimaryKey(), record);
  }

  public boolean saveIfAbset(String tableName, Row record) {
    return saveIfAbset(tableName, config.dialect.getDefaultPrimaryKey(), record);
  }

  /**
   * @param tableName
   * @param record
   * @param jsonFields
   * @return
   */
  public boolean save(String tableName, Row record, String[] jsonFields) {
    return save(tableName, config.dialect.getDefaultPrimaryKey(), record, jsonFields);
  }

  public boolean update(Config config, Connection conn, String tableName, String primaryKeys, Row record) {
    if (record.modifyFlag == null || record.modifyFlag.isEmpty()) {
      return false;
    }

    String[] pKeys = primaryKeys.split(",");
    Object[] ids = new Object[pKeys.length];

    for (int i = 0; i < pKeys.length; i++) {
      ids[i] = record.get(pKeys[i].trim()); // .trim() is important!
      if (ids[i] == null)
        throw new ActiveRecordException("You can't update record without Primary Key, " + pKeys[i] + " can not be null.");
    }

    StringBuilder sql = new StringBuilder();
    List<Object> paras = new ArrayList<Object>();
    config.dialect.forDbUpdate(tableName, pKeys, ids, record, sql, paras);

    if (paras.size() <= 1) { // 参数个数为 1 的情况表明只有主键，也无需更新
      return false;
    }

    int result = update(config, conn, sql.toString(), paras.toArray());
    if (result >= 1) {
      record.clearModifyFlag();
      return true;
    }
    return false;
  }

  public boolean update(Config config, Connection conn, String tableName, String primaryKey, Row record,
      //
      String[] jsonFields) {
    if (record.modifyFlag == null || record.modifyFlag.isEmpty()) {
      return false;
    }

    String[] pKeys = primaryKey.split(",");
    Object[] ids = new Object[pKeys.length];

    for (int i = 0; i < pKeys.length; i++) {
      ids[i] = record.get(pKeys[i].trim()); // .trim() is important!
      if (ids[i] == null) {
        throw new ActiveRecordException("You can't update record without Primary Key, " + pKeys[i] + " can not be null.");
      }

    }

    StringBuilder sql = new StringBuilder();
    List<Object> paras = new ArrayList<Object>();
    config.dialect.forDbUpdate(tableName, pKeys, ids, record, sql, paras, jsonFields);

    if (paras.size() <= 1) { // 参数个数为 1 的情况表明只有主键，也无需更新
      return false;
    }

    int result = update(config, conn, sql.toString(), paras.toArray());
    if (result >= 1) {
      record.clearModifyFlag();
      return true;
    }
    return false;
  }

  /**
   * Update Record.
   * 
   * <pre>
   * Example: Db.use().update("user_role", "user_id, role_id", record);
   * </pre>
   *
   * @param tableName  the table name of the Record save to
   * @param primaryKeys the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param record     the Record object
   */
  public boolean update(String tableName, String primaryKeys, Row record) {

    Connection conn = null;
    try {
      conn = config.getConnection();
      return update(config, conn, tableName, primaryKeys, record);
    } finally {
      config.close(conn);
    }
  }

  public boolean update(String tableName, String primaryKey, Row record, String[] jsonFields) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return update(config, conn, tableName, primaryKey, record, jsonFields);
    } finally {
      config.close(conn);
    }
  }

  /**
   * Update record with default primary key.
   * 
   * <pre>
   * Example: Db.use().update("user", record);
   * </pre>
   *
   * @see #update(String, String, Row)
   */
  public boolean update(String tableName, Row record) {
    return update(tableName, config.dialect.getDefaultPrimaryKey(), record);
  }

  /**
   *
   */
  public Object execute(ICallback callback) {
    return execute(config, callback);
  }

  /**
   * Execute callback. It is useful when all the API can not satisfy your
   * requirement.
   *
   * @param config   the Config object
   * @param callback the ICallback interface
   */
  public Object execute(Config config, ICallback callback) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      return callback.call(conn);
    } catch (Exception e) {
      throw new ActiveRecordException(e);
    } finally {
      config.close(conn);
    }
  }

  /**
   * Execute transaction.
   *
   * @param config           the Config object
   * @param transactionLevel the transaction level
   * @param atom             the atom operation
   * @return true if transaction executing succeed otherwise false
   */
  public boolean tx(Config config, int transactionLevel, IAtom atom) {
    Connection conn = config.getThreadLocalConnection();
    if (conn != null) { // Nested transaction support
      try {
        if (conn.getTransactionIsolation() < transactionLevel)
          conn.setTransactionIsolation(transactionLevel);
        boolean result = atom.run();
        if (result) {
          return true;
        }
        throw new NestedTransactionHelpException("Notice the outer transaction that the nested transaction return false"); // important:can not return false
      } catch (SQLException e) {
        throw new ActiveRecordException(e);
      }
    }
    Boolean autoCommit = null;
    try {
      conn = config.getConnection();
      autoCommit = conn.getAutoCommit();
      config.setThreadLocalConnection(conn);
      conn.setTransactionIsolation(transactionLevel);
      conn.setAutoCommit(false);
      boolean result = atom.run();
      if (result) {
        conn.commit();
      } else {
        conn.rollback();
      }
      return result;
    } catch (NestedTransactionHelpException e) {
      if (conn != null)
        try {
          conn.rollback();
        } catch (Exception e1) {
          log.error(e1.getMessage(), e1);
        }
      return false;
    } catch (Throwable t) {
      if (conn != null)
        try {
          conn.rollback();
        } catch (Exception e1) {
          log.error(e1.getMessage(), e1);
        }
      throw t instanceof ActiveRecordException ? (ActiveRecordException) t : new ActiveRecordException(t);
    } finally {
      try {
        if (conn != null) {
          if (autoCommit != null)
            conn.setAutoCommit(autoCommit);
          conn.close();
        }
      } catch (Throwable t) {
        log.error(t.getMessage(), t); // can not throw exception here, otherwise the more important exception in
                                      // previous catch block can not be thrown
      } finally {
        config.removeThreadLocalConnection(); // prevent memory leak
      }
    }
  }

  /**
   * Execute transaction with default transaction level.
   *
   * @see #tx(int, IAtom)
   */
  public boolean tx(IAtom atom) {
    return tx(config, config.getTransactionLevel(), atom);
  }

  public boolean tx(int transactionLevel, IAtom atom) {
    return tx(config, transactionLevel, atom);
  }

  /**
   * 主要用于嵌套事务场景
   * <p>
   * 实例：https://jfinal.com/feedback/4008
   * <p>
   * 默认情况下嵌套事务会被合并成为一个事务，那么内层与外层任何地方回滚事务 所有嵌套层都将回滚事务，也就是说嵌套事务无法独立提交与回滚
   * <p>
   * 使用 txInNewThread(...) 方法可以实现层之间的事务控制的独立性 由于事务处理是将 Connection 绑定到线程上的，所以
   * txInNewThread(...) 通过建立新线程来实现嵌套事务的独立控制
   */
  public Future<Boolean> txInNewThread(IAtom atom) {
    FutureTask<Boolean> task = new FutureTask<>(() -> tx(config, config.getTransactionLevel(), atom));
    Thread thread = new Thread(task);
    thread.setDaemon(true);
    thread.start();
    return task;
  }

  public Future<Boolean> txInNewThread(int transactionLevel, IAtom atom) {
    FutureTask<Boolean> task = new FutureTask<>(() -> tx(config, transactionLevel, atom));
    Thread thread = new Thread(task);
    thread.setDaemon(true);
    thread.start();
    return task;
  }

  /**
   * Find Record by cache.
   *
   * @param cacheName the cache name
   * @param key       the key used to get date from cache
   * @return the list of Record
   * @see #find(String, Object...)
   */
  public List<Row> findByCache(String cacheName, Object key, String sql, Object... paras) {
    IDbCache cache = config.getCache();
    List<Row> result = cache.get(cacheName, key);
    if (result == null) {
      result = find(sql, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  public <T> List<T> findByCache(Class<T> clazz, String cacheName, Object key, String sql, Object... paras) {
    IDbCache cache = config.getCache();
    List<T> result = cache.get(cacheName, key);
    if (result == null) {
      result = find(clazz, sql, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  /**
   * @see #findByCache(String, Object, String, Object...)
   */
  public List<Row> findByCache(String cacheName, Object key, String sql) {
    return findByCache(cacheName, key, sql, DbKit.NULL_PARA_ARRAY);
  }

  public <T> List<T> findByCache(Class<T> clazz, String cacheName, Object key, String sql) {
    return findByCache(clazz, cacheName, key, sql, DbKit.NULL_PARA_ARRAY);
  }

  /**
   * Find first record by cache. I recommend add "limit 1" in your sql.
   *
   * @param cacheName the cache name
   * @param key       the key used to get date from cache
   * @param sql       an SQL statement that may contain one or more '?' IN
   *                  parameter placeholders
   * @param paras     the parameters of sql
   * @return the Record object
   * @see #findFirst(String, Object...)
   */
  public Row findFirstByCache(String cacheName, Object key, String sql, Object... paras) {
    IDbCache cache = config.getCache();
    Row result = cache.get(cacheName, key);
    if (result == null) {
      result = findFirst(sql, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  public Row findFirstByCache(String cacheName, Object key, int ttl, String sql, Object... paras) {
    IDbCache cache = config.getCache();
    Row result = cache.get(cacheName, key);
    if (result == null) {
      result = findFirst(sql, paras);
      cache.put(cacheName, key, result, ttl);
    }
    return result;
  }

  public <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, String sql, Object... paras) {
    IDbCache cache = config.getCache();
    T result = cache.get(cacheName, key);
    if (result == null) {
      result = findFirst(clazz, sql, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  public <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, int ttl, String sql, Object... paras) {
    IDbCache cache = config.getCache();
    T result = cache.get(cacheName, key);
    if (result == null) {
      result = findFirst(clazz, sql, paras);
      cache.put(cacheName, key, result, ttl);
    }
    return result;
  }

  /**
   * @see #findFirstByCache(String, Object, String, Object...)
   */
  public Row findFirstByCache(String cacheName, Object key, String sql) {
    return findFirstByCache(cacheName, key, sql, DbKit.NULL_PARA_ARRAY);
  }

  public Row findFirstByCache(String cacheName, Object key, int ttl, String sql) {
    return findFirstByCache(cacheName, key, ttl, sql, DbKit.NULL_PARA_ARRAY);
  }

  public <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, String sql) {
    return findFirstByCache(clazz, cacheName, key, sql, DbKit.NULL_PARA_ARRAY);
  }

  public <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, int ttl, String sql) {
    return findFirstByCache(clazz, cacheName, key, sql, ttl, DbKit.NULL_PARA_ARRAY);
  }

  public <T> Page<T> doPaginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, Boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    IDbCache cache = config.getCache();
    Page<T> result = cache.get(cacheName, key);
    if (result == null) {
      result = doPaginate(clazz, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  public Page<Row> doPaginateByCache(String cacheName, Object key, int pageNumber, int pageSize, Boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    IDbCache cache = config.getCache();
    Page<Row> result = cache.get(cacheName, key);
    if (result == null) {
      result = doPaginate(pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  public Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    assert sqls != null;
    return doPaginateByCache(cacheName, key, pageNumber, pageSize, null, sqls[0], sqls[1], sqlPara.getPara());
  }

  public Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    assert sqls != null;
    return doPaginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, sqls[0], sqls[1], sqlPara.getPara());
  }

  /**
   * @see #paginateByCache(String, Object, int, int, String, String, Object...)
   */
  public Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, String select, String sqlExceptSelect) {
    return doPaginateByCache(cacheName, key, pageNumber, pageSize, null, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  public Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect) {
    return doPaginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  /**
   * Paginate by cache.
   *
   * @return Page
   * @see #paginate(int, int, String, String, Object...)
   */
  public Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) {
    return doPaginateByCache(cacheName, key, pageNumber, pageSize, null, select, sqlExceptSelect, paras);
  }

  public Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    return doPaginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
  }

  public Page<Row> paginateByCacheByFullSql(String cacheName, Object key, int pageNumber, int pageSize, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByCacheByFullSql(cacheName, key, pageNumber, pageSize, null, totalRowSql, findSql, paras);
  }

  public Page<Row> paginateByCacheByFullSql(String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByCacheByFullSql(cacheName, key, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
  }

  private Page<Row> doPaginateByCacheByFullSql(String cacheName, Object key, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    IDbCache cache = config.getCache();
    Page<Row> result = cache.get(cacheName, key);
    if (result == null) {
      result = doPaginateByFullSql(pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  public <T> Page<T> paginateByCacheByFullSql(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByCacheByFullSql(clazz, cacheName, key, pageNumber, pageSize, null, totalRowSql, findSql, paras);
  }

  public <T> Page<T> paginateByCacheByFullSql(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    return doPaginateByCacheByFullSql(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
  }

  private <T> Page<T> doPaginateByCacheByFullSql(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, String findSql,
      Object... paras) {
    IDbCache cache = config.getCache();
    Page<T> result = cache.get(cacheName, key);
    if (result == null) {
      result = doPaginateByFullSql(clazz, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
      cache.put(cacheName, key, result);
    }
    return result;
  }

  public <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    assert sqls != null;
    return doPaginateByCache(clazz, cacheName, key, pageNumber, pageSize, null, sqls[0], sqls[1], sqlPara.getPara());
  }

  public <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    assert sqls != null;
    return doPaginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, sqls[0], sqls[1], sqlPara.getPara());
  }

  public <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, String select, String sqlExceptSelect) {
    return doPaginateByCache(clazz, cacheName, key, pageNumber, pageSize, null, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  public <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect) {
    return doPaginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, DbKit.NULL_PARA_ARRAY);
  }

  public <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) {
    return doPaginateByCache(clazz, cacheName, key, pageNumber, pageSize, null, select, sqlExceptSelect, paras);
  }

  public <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize, boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    return doPaginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
  }

  public int[] batch(Config config, Connection conn, String sql, Object[][] paras, int batchSize) throws SQLException {
    if (paras == null || paras.length == 0)
      return new int[0];
    if (batchSize < 1)
      throw new IllegalArgumentException("The batchSize must more than 0.");

    boolean isInTransaction = config.isInTransaction();
    int counter = 0;
    int pointer = 0;
    int[] result = new int[paras.length];
    try (PreparedStatement pst = conn.prepareStatement(sql)) {
      for (Object[] para : paras) {
        for (int j = 0; j < para.length; j++) {
          Object value = para[j];
          if (value instanceof java.util.Date) {
            if (value instanceof java.sql.Date) {
              pst.setDate(j + 1, (java.sql.Date) value);
            } else if (value instanceof java.sql.Timestamp) {
              pst.setTimestamp(j + 1, (java.sql.Timestamp) value);
            } else {
              // Oracle、SqlServer 中的 TIMESTAMP、DATE 支持 new Date() 给值
              java.util.Date d = (java.util.Date) value;
              pst.setTimestamp(j + 1, new java.sql.Timestamp(d.getTime()));
            }
          } else {
            pst.setObject(j + 1, value);
          }
        }
        pst.addBatch();
        if (++counter >= batchSize) {
          counter = 0;
          long start = System.currentTimeMillis();
          int[] r = pst.executeBatch();
          ISqlStatementStat stat = config.getSqlStatementStat();
          if (stat != null) {
            long end = System.currentTimeMillis();
            long elapsed = end - start;
            stat.save(config.name, "batch", sql, paras, r.length, start, elapsed, config.writeSync);
          }
          if (!isInTransaction)
            conn.commit();
          for (int i : r) {
            result[pointer++] = i;
          }
        }
      }
      if (counter != 0) {
        long start = System.currentTimeMillis();
        int[] r = pst.executeBatch();
        ISqlStatementStat stat = config.getSqlStatementStat();
        if (stat != null) {
          long end = System.currentTimeMillis();
          long elapsed = end - start;
          stat.save(config.name, "batch", sql, paras, r.length, start, elapsed, config.writeSync);
        }
        if (!isInTransaction) {
          conn.commit();
        }

        for (int i : r) {
          result[pointer++] = i;
        }
      }

      return result;
    }
  }

  /**
   * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
   * 
   * <pre>
   * Example:
   * String sql = "insert into user(name, cash) values(?, ?)";
   * int[] result = Db.use().batch(sql, new Object[][]{{"James", 888}, {"zhanjin", 888}});
   * </pre>
   *
   * @param sql   The SQL to execute.
   * @param paras An array of query replacement parameters. Each row in this array
   *              is one set of batch replacement values.
   * @return The number of rows updated per statement
   */
  public int[] batch(String sql, Object[][] paras, int batchSize) {
    Connection conn = null;
    Boolean autoCommit = null;
    try {
      conn = config.getConnection();
      autoCommit = conn.getAutoCommit();
      conn.setAutoCommit(false);
      return batch(config, conn, sql, paras, batchSize);
    } catch (Exception e) {
      throw new ActiveRecordException(e.getMessage(), sql, e);
    } finally {
      if (autoCommit != null) {
        try {
          conn.setAutoCommit(autoCommit);
        } catch (Exception e) {
          throw new ActiveRecordException(e.getMessage(), sql, e);
        }
      }
      config.close(conn);
    }
  }

  public int[] batch(Config config, Connection conn, String sql, String columns, List list, int batchSize) {
    if (list == null || list.size() == 0) {
      return new int[0];
    }

    Object element = list.get(0);
    if (!(element instanceof Row) && !(element instanceof Model)) {
      throw new IllegalArgumentException("The element in list must be Model or Row.");
    }

    if (batchSize < 1) {
      throw new IllegalArgumentException("The batchSize must more than 0.");
    }

    boolean isModel = element instanceof Model;

    String[] columnArray = columns.split(",");
    for (int i = 0; i < columnArray.length; i++) {
      columnArray[i] = columnArray[i].trim();
    }

    boolean isInTransaction = config.isInTransaction();
    int counter = 0;
    int pointer = 0;
    int size = list.size();
    int[] result = new int[size];
    PreparedStatement pst = null;
    try {
      pst = conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new ActiveRecordException(e.getMessage(), sql, e);
    }
    try {
      for (Object o : list) {
        Map map = isModel ? ((Model) o)._getAttrs() : ((Row) o).getColumns();
        for (int j = 0; j < columnArray.length; j++) {
          String fields = columnArray[j];
          Object value = map.get(fields);
          if (value instanceof java.util.Date) {
            if (value instanceof java.sql.Date) {
              try {
                pst.setDate(j + 1, (java.sql.Date) value);
              } catch (SQLException e) {
                throw new ActiveRecordException(fields + "=" + value.toString(), sql, e);
              }
            } else if (value instanceof java.sql.Timestamp) {
              try {
                pst.setTimestamp(j + 1, (java.sql.Timestamp) value);
              } catch (SQLException e) {
                throw new ActiveRecordException(fields + "=" + value.toString(), sql, e);
              }
            } else {
              // Oracle、SqlServer 中的 TIMESTAMP、DATE 支持 new Date() 给值
              java.util.Date d = (java.util.Date) value;
              try {
                pst.setTimestamp(j + 1, new java.sql.Timestamp(d.getTime()));
              } catch (SQLException e) {
                throw new ActiveRecordException(fields + "=" + value.toString(), sql, e);
              }
            }
          } else {
            try {
              pst.setObject(j + 1, value);
            } catch (SQLException e) {
              throw new ActiveRecordException(fields + "=" + value.toString(), sql, e);
            }
          }
        }
        try {
          pst.addBatch();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, e);
        }
        if (++counter >= batchSize) {
          counter = 0;
          long start = System.currentTimeMillis();
          int[] r = null;
          try {
            r = pst.executeBatch();
          } catch (SQLException e1) {
            throw new ActiveRecordException(e1.getMessage(), sql, e1);
          }
          ISqlStatementStat stat = config.getSqlStatementStat();
          if (stat != null) {
            long end = System.currentTimeMillis();
            long elapsed = end - start;
            stat.save(config.name, "batch", sql, list.toArray(), r.length, start, elapsed, config.writeSync);
          }
          if (!isInTransaction)
            try {
              conn.commit();
            } catch (SQLException e) {
              throw new ActiveRecordException(e.getMessage(), sql, e);
            }
          for (int i : r) {
            result[pointer++] = i;
          }
        }
      }
      if (counter != 0) {
        long start = System.currentTimeMillis();
        int[] r = null;
        try {
          r = pst.executeBatch();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, e);
        }
        ISqlStatementStat stat = config.getSqlStatementStat();
        if (stat != null) {
          long end = System.currentTimeMillis();
          long elapsed = end - start;
          stat.save(config.name, "batch", sql, list.toArray(), r.length, start, elapsed, config.writeSync);
        }
        if (!isInTransaction)
          try {
            conn.commit();
          } catch (SQLException e) {
            throw new ActiveRecordException(e.getMessage(), sql, e);
          }
        for (int i : r) {
          result[pointer++] = i;
        }
      }

      return result;
    } finally {
      if (pst != null) {
        try {
          pst.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e.getMessage(), sql, e);
        }
      }
    }
  }

  /**
   * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
   * 
   * <pre>
   * Example:
   * String sql = "insert into user(name, cash) values(?, ?)";
   * int[] result = Db.use().batch(sql, "name, cash", modelList, 500);
   * </pre>
   *
   * @param sql               The SQL to execute.
   * @param columns           the columns need be processed by sql.
   * @param modelOrRecordList model or record object list.
   * @param batchSize         batch size.
   * @return The number of rows updated per statement
   */
  public int[] batch(String sql, String columns, List modelOrRecordList, int batchSize) {
    Connection conn = null;
    Boolean autoCommit = null;
    try {
      conn = config.getConnection();
      try {
        autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, e);
      }

      return batch(config, conn, sql, columns, modelOrRecordList, batchSize);
    } finally {
      if (autoCommit != null) {
        try {
          conn.setAutoCommit(autoCommit);
        } catch (Exception e) {
          throw new ActiveRecordException(e.getMessage(), sql, e);
        }
      }
      config.close(conn);
    }
  }

  public int[] batch(String sql, String columns, String[] jsonFields, List<Row> modelOrRecordList, int batchSize) {
    Connection conn = null;
    Boolean autoCommit = null;
    try {
      conn = config.getConnection();
      try {
        autoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
      } catch (SQLException e) {
        throw new ActiveRecordException(e.getMessage(), sql, e);
      }
      config.dialect.transformJsonFields(modelOrRecordList, jsonFields);
      return batch(config, conn, sql, columns, modelOrRecordList, batchSize);
    } finally {
      if (autoCommit != null)
        try {
          conn.setAutoCommit(autoCommit);
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      config.close(conn);
    }
  }

  public int[] batch(Config config, Connection conn, List<String> sqlList, int batchSize) throws SQLException {
    if (sqlList == null || sqlList.size() == 0)
      return new int[0];
    if (batchSize < 1)
      throw new IllegalArgumentException("The batchSize must more than 0.");

    boolean isInTransaction = config.isInTransaction();
    int counter = 0;
    int pointer = 0;
    int size = sqlList.size();
    int[] result = new int[size];
    try (Statement st = conn.createStatement()) {
      for (String s : sqlList) {
        st.addBatch(s);
        if (++counter >= batchSize) {
          counter = 0;
          int[] r = st.executeBatch();
          if (!isInTransaction)
            conn.commit();
          for (int i : r) {
            result[pointer++] = i;
          }
        }
      }
      if (counter != 0) {
        int[] r = st.executeBatch();
        if (!isInTransaction)
          conn.commit();
        for (int i : r) {
          result[pointer++] = i;
        }
      }

      return result;
    }
  }

  /**
   * Execute a batch of SQL INSERT, UPDATE, or DELETE queries.
   * 
   * <pre>
   * Example:
   * int[] result = Db.use().batch(sqlList, 500);
   * </pre>
   *
   * @param sqlList   The SQL list to execute.
   * @param batchSize batch size.
   * @return The number of rows updated per statement
   */
  public int[] batch(List<String> sqlList, int batchSize) {
    Connection conn = null;
    Boolean autoCommit = null;
    try {
      conn = config.getConnection();
      autoCommit = conn.getAutoCommit();
      conn.setAutoCommit(false);
      return batch(config, conn, sqlList, batchSize);
    } catch (Exception e) {
      throw new ActiveRecordException(e.getMessage(), e);
    } finally {
      if (autoCommit != null)
        try {
          conn.setAutoCommit(autoCommit);
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      config.close(conn);
    }
  }

  /**
   * Batch save models using the "insert into ..." sql generated by the first
   * model in modelList. Ensure all the models can use the same sql as the first
   * model.
   */
  public int[] batchSave(List<? extends Model> modelList, int batchSize) {
    if (modelList == null || modelList.size() == 0) {
      return new int[0];
    }

    Model model = modelList.get(0);
    Map<String, Object> attrs = model._getAttrs();
    int index = 0;
    StringBuilder columns = new StringBuilder();
    // the same as the iterator in Dialect.forModelSave() to ensure the order of the
    // attrs
    for (Entry<String, Object> e : attrs.entrySet()) {
      if (config.dialect.isOracle()) { // 支持 oracle 自增主键
        Object value = e.getValue();
        if (value instanceof String && ((String) value).endsWith(".nextval")) {
          continue;
        }
      }

      if (index++ > 0) {
        columns.append(',');
      }
      columns.append(e.getKey());
    }

    StringBuilder sql = new StringBuilder();
    List<Object> parasNoUse = new ArrayList<Object>();
    Table table = model._getTable();

    config.dialect.forModelSave(table, attrs, sql, parasNoUse);
    return batch(sql.toString(), columns.toString(), modelList, batchSize);
  }

  /**
   * Batch save records using the "insert into ..." sql generated by the first
   * record in recordList. Ensure all the record can use the same sql as the first
   * record.
   *
   * @param tableName the table name
   */
  public int[] batchSave(String tableName, List<? extends Row> recordList, int batchSize) {
    if (recordList == null || recordList.size() == 0) {
      return new int[0];
    }

    Row record = recordList.get(0);
    Map<String, Object> cols = record.getColumns();
    int index = 0;
    StringBuilder columns = new StringBuilder();
    // the same as the iterator in Dialect.forDbSave() to ensure the order of the
    // columns
    for (Entry<String, Object> e : cols.entrySet()) {
      if (config.dialect.isOracle()) { // 支持 oracle 自增主键
        Object value = e.getValue();
        if (value instanceof String && ((String) value).endsWith(".nextval")) {
          continue;
        }
      }

      if (index++ > 0) {
        columns.append(',');
      }
      columns.append(e.getKey());
    }

    String[] pKeysNoUse = new String[0];
    StringBuilder sql = new StringBuilder();
    List<Object> parasNoUse = new ArrayList<Object>();
    config.dialect.forDbSave(tableName, pKeysNoUse, record, sql, parasNoUse);
    return batch(sql.toString(), columns.toString(), recordList, batchSize);
  }

  public int[] batchSave(String tableName, String[] jsonFields, List<Row> recordList, int batchSize) {
    if (recordList == null || recordList.size() == 0) {
      return new int[0];
    }

    Row record = recordList.get(0);
    Map<String, Object> cols = record.getColumns();
    int index = 0;
    StringBuilder columns = new StringBuilder();
    // the same as the iterator in Dialect.forDbSave() to ensure the order of the
    // columns
    for (Entry<String, Object> e : cols.entrySet()) {
      if (config.dialect.isOracle()) { // 支持 oracle 自增主键
        Object value = e.getValue();
        if (value instanceof String && ((String) value).endsWith(".nextval")) {
          continue;
        }
      }

      if (index++ > 0) {
        columns.append(',');
      }
      columns.append(e.getKey());
    }

    String[] pKeysNoUse = new String[0];
    StringBuilder sql = new StringBuilder();
    List<Object> parasNoUse = new ArrayList<Object>();
    config.dialect.forDbSave(tableName, pKeysNoUse, record, sql, parasNoUse);
    return batch(sql.toString(), columns.toString(), jsonFields, recordList, batchSize);
  }

  public int[] batchDelete(String tableName, List<? extends Row> recordList, int batchSize) {
    if (recordList == null || recordList.size() == 0) {
      return new int[0];
    }

    Row record = recordList.get(0);
    Map<String, Object> cols = record.getColumns();
    int index = 0;
    StringBuilder columns = new StringBuilder();
    // the same as the iterator in Dialect.forDbSave() to ensure the order of the
    // columns
    for (Entry<String, Object> e : cols.entrySet()) {
      if (config.dialect.isOracle()) { // 支持 oracle 自增主键
        Object value = e.getValue();
        if (value instanceof String && ((String) value).endsWith(".nextval")) {
          continue;
        }
      }

      if (index++ > 0) {
        columns.append(',');
      }
      columns.append(e.getKey());
    }

    String[] pKeysNoUse = new String[0];
    StringBuilder sql = new StringBuilder();
    List<Object> parasNoUse = new ArrayList<Object>();
    config.dialect.forDbDelete(tableName, pKeysNoUse, record, sql, parasNoUse);

    return batch(sql.toString(), columns.toString(), recordList, batchSize);
  }

  /**
   * Batch update models using the attrs names of the first model in modelList.
   * Ensure all the models can use the same sql as the first model.
   */
  public int[] batchUpdate(List<? extends Model> modelList, int batchSize) {
    if (modelList == null || modelList.size() == 0)
      return new int[0];

    Model model = modelList.get(0);

    // 新增支持 modifyFlag
    if (model.modifyFlag == null || model.modifyFlag.isEmpty()) {
      return new int[0];
    }
    Set<String> modifyFlag = model._getModifyFlag();

    Table table = model._getTable();
    String[] pKeys = table.getPrimaryKey();
    Map<String, Object> attrs = model._getAttrs();
    List<String> attrNames = new ArrayList<String>();
    // the same as the iterator in Dialect.forModelSave() to ensure the order of the
    // attrs
    for (Entry<String, Object> e : attrs.entrySet()) {
      String attr = e.getKey();
      if (modifyFlag.contains(attr) && !config.dialect.isPrimaryKey(attr, pKeys) && table.hasColumnLabel(attr)) {
        attrNames.add(attr);

      }
    }
    for (String pKey : pKeys) {
      attrNames.add(pKey);
    }
    String columns = StrKit.join(attrNames.toArray(new String[attrNames.size()]), ",");

    // update all attrs of the model not use the midifyFlag of every single model
    // Set<String> modifyFlag = attrs.keySet(); // model.getModifyFlag();

    StringBuilder sql = new StringBuilder();
    List<Object> parasNoUse = new ArrayList<Object>();
    config.dialect.forModelUpdate(model._getTable(), attrs, modifyFlag, sql, parasNoUse);
    return batch(sql.toString(), columns, modelList, batchSize);
  }

  /**
   * Batch update records using the columns names of the first record in
   * recordList. Ensure all the records can use the same sql as the first record.
   *
   * @param tableName  the table name
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   */
  public int[] batchUpdate(String tableName, String primaryKey, List<? extends Row> recordList, int batchSize) {
    if (recordList == null || recordList.size() == 0)
      return new int[0];

    String[] pKeys = primaryKey.split(",");
    config.dialect.trimPrimaryKeys(pKeys);

    Row record = recordList.get(0);

    // Record 新增支持 modifyFlag
    if (record.modifyFlag == null || record.modifyFlag.isEmpty()) {
      return new int[0];
    }
    Set<String> modifyFlag = record._getModifyFlag();

    Map<String, Object> cols = record.getColumns();
    List<String> colNames = new ArrayList<String>();
    // the same as the iterator in Dialect.forDbUpdate() to ensure the order of the
    // columns
    for (Entry<String, Object> e : cols.entrySet()) {
      String col = e.getKey();
      if (modifyFlag.contains(col) && !config.dialect.isPrimaryKey(col, pKeys))
        colNames.add(col);
    }
    for (String pKey : pKeys)
      colNames.add(pKey);
    String columns = StrKit.join(colNames.toArray(new String[colNames.size()]), ",");

    Object[] idsNoUse = new Object[pKeys.length];
    StringBuilder sql = new StringBuilder();
    List<Object> parasNoUse = new ArrayList<Object>();
    config.dialect.forDbUpdate(tableName, pKeys, idsNoUse, record, sql, parasNoUse);
    return batch(sql.toString(), columns, recordList, batchSize);
  }

  /**
   * Batch update records with default primary key, using the columns names of the
   * first record in recordList. Ensure all the records can use the same sql as
   * the first record.
   *
   * @param tableName the table name
   */
  public int[] batchUpdate(String tableName, List<? extends Row> recordList, int batchSize) {
    return batchUpdate(tableName, config.dialect.getDefaultPrimaryKey(), recordList, batchSize);
  }

  public String getSql(String key) {
    return config.getSqlKit().getSql(key);
  }

  // 支持传入变量用于 sql 生成。为了避免用户将参数拼接在 sql 中引起 sql 注入风险，只在 SqlKit 中开放该功能
  // public String getSql(String key, Map data) {
  // return config.getSqlKit().getSql(key, data);
  // }

  public SqlPara getSqlPara(String key, Row record) {
    return getSqlPara(key, record.getColumns());
  }

  public SqlPara getSqlPara(String key, Model model) {
    return getSqlPara(key, model._getAttrs());
  }

  public SqlPara getSqlPara(String key, Map data) {
    return config.getSqlKit().getSqlPara(key, data);
  }

  public SqlPara getSqlPara(String key, Object... paras) {
    return config.getSqlKit().getSqlPara(key, paras);
  }

  public SqlPara getSqlParaByString(String content, Map data) {
    return config.getSqlKit().getSqlParaByString(content, data);
  }

  public SqlPara getSqlParaByString(String content, Object... paras) {
    return config.getSqlKit().getSqlParaByString(content, paras);
  }

  public List<Row> find(SqlPara sqlPara) {
    return find(sqlPara.getSql(), sqlPara.getPara());
  }

  public <T> List<T> find(Class<T> clazz, SqlPara sqlPara) {
    return find(clazz, sqlPara.getSql(), sqlPara.getPara());
  }

  public Row findFirst(SqlPara sqlPara) {
    return findFirst(sqlPara.getSql(), sqlPara.getPara());
  }

  public <T> T findFirst(Class<T> clazz, SqlPara sqlPara) {
    return findFirst(clazz, sqlPara.getSql(), sqlPara.getPara());
  }

  public int update(SqlPara sqlPara) {
    return update(sqlPara.getSql(), sqlPara.getPara());
  }

  public Page<Row> paginate(int pageNumber, int pageSize, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    return doPaginate(pageNumber, pageSize, null, sqls[0], sqls[1], sqlPara.getPara());
  }

  public <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    return doPaginate(clazz, pageNumber, pageSize, null, sqls[0], sqls[1], sqlPara.getPara());
  }

  public Page<Row> paginate(int pageNumber, int pageSize, boolean isGroupBySql, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    return doPaginate(pageNumber, pageSize, isGroupBySql, sqls[0], sqls[1], sqlPara.getPara());
  }

  public <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql, SqlPara sqlPara) {
    String[] sqls = PageSqlKit.parsePageSql(sqlPara.getSql());
    return doPaginate(clazz, pageNumber, pageSize, isGroupBySql, sqls[0], sqls[1], sqlPara.getPara());
  }

  // ---------

  /**
   * 迭代处理每一个查询出来的 Record 对象
   * 
   * <pre>
   * 例子：
   * Db.each(record -> {
   *    // 处理 record 的代码在此
   *
   *    // 返回 true 继续循环处理下一条数据，返回 false 立即终止循环
   *    return true;
   * }, sql, paras);
   * </pre>
   */
  public void each(Function<Row, Boolean> func, String sql, Object... paras) {
    Connection conn = null;
    try {
      conn = config.getConnection();
      try (PreparedStatement pst = conn.prepareStatement(sql)) {
        config.dialect.fillStatement(pst, paras);
        long start = System.currentTimeMillis();
        try (ResultSet rs = pst.executeQuery()) {
          config.dialect.eachRecord(config, rs, func);
          ISqlStatementStat stat = config.getSqlStatementStat();
          if (stat != null) {
            long end = System.currentTimeMillis();
            long elapsed = end - start;
            stat.save(config.name, "batch", sql, paras, -1, start, elapsed, config.writeSync);
          }
        }
      }

    } catch (Exception e) {
      throw new ActiveRecordException(e.getMessage(), sql, e);
    } finally {
      config.close(conn);
    }
  }

  // ---------

  public DbTemplate template(String key, Map data) {
    return new DbTemplate(this, key, data);
  }

  public DbTemplate template(String key, Object... paras) {
    return new DbTemplate(this, key, paras);
  }

  // ---------

  public DbTemplate templateByString(String content, Map data) {
    return new DbTemplate(true, this, content, data);
  }

  public DbTemplate templateByString(String content, Object... paras) {
    return new DbTemplate(true, this, content, paras);
  }

  public boolean exists(String sql, Object[] paras) {
    Long size = Db.queryLong(sql, paras);
    if (size > 0) {
      return true;
    } else {
      return false;
    }
  }

  public boolean exists(String tableName, String fields, Object... paras) {
    String sql = config.dialect.forExistsByFields(tableName, fields);
    return this.exists(sql, paras);
  }

  public Long count(String sql) {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("SELECT count(*) from (").append(sql).append(") AS subquery;");
    return Db.queryLong(sql, stringBuffer.toString());
  }

  public Long countTable(String table) {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("SELECT count(*) from ").append(table).append(";");
    return Db.queryLong(stringBuffer.toString());
  }

  public Long countBySql(String sql, Object... paras) {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("SELECT count(*) from (").append(sql).append(") AS subquery;");
    return Db.queryLong(stringBuffer.toString(), paras);
  }
}
