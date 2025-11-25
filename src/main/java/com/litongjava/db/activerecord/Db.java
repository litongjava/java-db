package com.litongjava.db.activerecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.postgresql.util.PGobject;

import com.jfinal.kit.SyncWriteMap;
import com.litongjava.db.SqlPara;
import com.litongjava.kit.DbTableNameUtils;
import com.litongjava.model.db.IAtom;
import com.litongjava.model.db.ICallback;
import com.litongjava.model.page.Page;

/**
 * Db. Powerful database query and update tool box.
 */
@SuppressWarnings("rawtypes")
public class Db {

  private static DbPro MAIN = null;
  private static List<DbPro> replicas = null;
  private static int replicaSize = 0;

  private static final AtomicInteger counter = new AtomicInteger(0);

  private static final Map<String, DbPro> cache = new SyncWriteMap<String, DbPro>(32, 0.25F);
  private static final Map<String, DbPro> replicaCaches = new SyncWriteMap<String, DbPro>(32, 0.25F);

  /**
   * for DbKit.addConfig(configName)
   */
  static void init(String configName) {
    MAIN = DbKit.getConfig(configName).dbProFactory.getDbPro(configName); // new DbPro(configName);
    cache.put(configName, MAIN);
  }

  public static void initReplicas(List<Config> replicaConfigs) {
    replicaSize = replicaConfigs.size();
    replicas = new ArrayList<>(replicaSize);
    for (Config config : replicaConfigs) {
      String configName = config.getName();
      DbPro dbPro = config.dbProFactory.getDbPro(configName);
      replicaCaches.put(configName, dbPro);
      replicas.add(dbPro);
    }
  }

  /**
   * for DbKit.removeConfig(configName)
   */
  static void removeDbProWithConfig(String configName) {
    if (MAIN != null && MAIN.config.getName().equals(configName)) {
      MAIN = null;
    }
    cache.remove(configName);
  }

  public static DbPro use(String configName) {
    DbPro result = cache.get(configName);
    if (result == null) {
      Config config = DbKit.getConfig(configName);
      if (config == null) {
        throw new IllegalArgumentException("Config not found by configName: " + configName);
      }
      result = config.dbProFactory.getDbPro(configName); // new DbPro(configName);
      cache.put(configName, result);
    }
    return result;
  }

  public static DbPro use() {
    return MAIN;
  }

  public static DbPro useRead() {
    if (replicas != null) {
      return useReplica();
    }
    return MAIN;
  }

  public static DbPro useReplica(String configName) {
    DbPro result = replicaCaches.get(configName);
    if (result == null) {
      Config config = DbKit.getConfig(configName);
      if (config == null) {
        throw new IllegalArgumentException("Config not found by configName: " + configName);
      }
      result = config.dbProFactory.getDbPro(configName); // new DbPro(configName);
      replicaCaches.put(configName, result);
    }
    return result;
  }

  public static DbPro useReplica() {
    int index = counter.getAndIncrement() % replicaSize;
    return replicas.get(index);
  }

  public static DbPro useReplica(int i) {
    return replicas.get(i);
  }

  // =================================================save================================================
  public static boolean save(Row r) {
    return MAIN.save(r.getTableName(), r);
  }

  /**
   * @param config
   * @param conn
   * @param tableName
   * @param primaryKey
   * @param record
   * @return
   * @throws SQLException
   */
  static boolean save(Config config, Connection conn, String tableName, String primaryKey, Row record)
      throws SQLException {
    return MAIN.save(config, conn, tableName, primaryKey, record);
  }

  /**
   * Save record.
   * 
   * <pre>
   * Example:
   * Record userRole = new Record().set("user_id", 123).set("role_id", 456);
   * Db.save("user_role", "user_id, role_id", userRole);
   * </pre>
   * 
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param record     the record will be saved
   * @param true       if save succeed otherwise false
   */
  public static boolean save(String tableName, String primaryKey, Row record) {
    return MAIN.save(tableName, primaryKey, record);
  }

  public static boolean save(String sql, Object... paras) {
    return MAIN.save(sql, paras);
  }

  /**
   * @see #save(String, String, Row)
   */
  public static boolean save(String tableName, Row record) {
    return MAIN.save(tableName, record);
  }

  public static boolean saveIfAbset(String tableName, Row record) {
    return MAIN.saveIfAbset(tableName, record);
  }

  /**
   * @param tableName
   * @param record
   * @param jsonFields
   * @return
   */
  public static boolean save(String tableName, Row record, String[] jsonFields) {
    return MAIN.save(tableName, record, jsonFields);
  }

  public static boolean save(String tableName, String primaryKey, Row record, String[] jsonFields) {
    return MAIN.save(tableName, primaryKey, record, jsonFields);
  }

  // ===================================================update==================================
  /**
   * Execute sql update
   */
  public static int update(Config config, Connection conn, String sql, Object... paras) throws SQLException {
    return MAIN.update(config, conn, sql, paras);
  }

  /**
   * 
   * @param sqlPara
   * @return
   */
  public static int update(SqlPara sqlPara) {
    return MAIN.update(sqlPara);
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
  public static int update(String sql, Object... paras) {
    return MAIN.update(sql, paras);
  }
  
  public static int updateBySql(String sql, Object... paras) {
    return MAIN.update(sql, paras);
  }

  /**
   * @see #update(String, Object...)
   * @param sql an SQL statement
   */
  public static int update(String sql) {
    return MAIN.update(sql);
  }

  /**
   * @param config
   * @param conn
   * @param tableName
   * @param primaryKey
   * @param record
   * @return
   * @throws SQLException
   */
  static boolean update(Config config, Connection conn, String tableName, String primaryKey, Row record)
      throws SQLException {
    return MAIN.update(config, conn, tableName, primaryKey, record);
  }

  public static boolean update(Row row) {
    return MAIN.update(row.getTableName(), "id", row);
  }

  /**
   * Update Record.
   * 
   * <pre>
   * Example: Db.update("user_role", "user_id, role_id", record);
   * </pre>
   * 
   * @param tableName   the table name of the Record save to
   * @param primaryKeys the primary key of the table, composite primary key is
   *                    separated by comma character: ","
   * @param record      the Record object
   * @param true        if update succeed otherwise false
   */
  public static boolean update(String tableName, String primaryKeys, Row record) {
    return MAIN.update(tableName, primaryKeys, record);
  }

  /**
   * 
   * @param tableName
   * @param primaryKey
   * @param record
   * @return
   */
  public static boolean update(String tableName, String primaryKey, Row record, String[] jsonFields) {
    return MAIN.update(tableName, primaryKey, record, jsonFields);
  }

  /**
   * Update record with default primary key.
   * 
   * <pre>
   * Example: Db.update("user", record);
   * </pre>
   * 
   * @see #update(String, String, Row)
   */
  public static boolean update(String tableName, Row record) {
    return MAIN.update(tableName, record);
  }

  // =======================================delete==============

  /**
   * Delete record by id with default primary key.
   * 
   * <pre>
   * Example: Db.deleteById("user", 15);
   * </pre>
   * 
   * @param tableName the table name of the table
   * @param idValue   the id value of the record
   * @return true if delete succeed otherwise false
   */
  public static boolean deleteById(String tableName, Object idValue) {
    return MAIN.deleteById(tableName, idValue);
  }

  /**
   * @param tableName
   * @param primaryKey
   * @param idValue
   * @return
   */
  public static boolean deleteById(String tableName, String primaryKey, Object idValue) {
    return MAIN.deleteById(tableName, primaryKey, idValue);
  }

  /**
   * Delete record by ids.
   * 
   * <pre>
   * Example: Db.deleteByIds("user", "user_id", 15);
   * Db.deleteByIds("user_role", "user_id, role_id", 123, 456);
   * </pre>
   * 
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param idValues   the id value of the record, it can be composite id values
   * @return true if delete succeed otherwise false
   */
  public static boolean deleteByIds(String tableName, String primaryKey, Object... idValues) {
    return MAIN.deleteByIds(tableName, primaryKey, idValues);
  }

  /**
   * Delete record.
   * 
   * <pre>
   * Example:
   * boolean succeed = Db.delete("user", "id", user);
   * </pre>
   * 
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param record     the record
   * @return true if delete succeed otherwise false
   */
  public static boolean delete(String tableName, String primaryKey, Row record) {
    return MAIN.delete(tableName, primaryKey, record);
  }

  /**
   * <pre>
   * Example:
   * boolean succeed = Db.delete("user", user);
   * </pre>
   * 
   * @see #delete(String, String, Row)
   */
  public static boolean deleteByIds(String tableName, Row record) {
    return MAIN.deleteByIds(tableName, record);
  }

  public static boolean delete(String tableName, Row record) {
    return MAIN.delete(tableName, record);
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
  public static int delete(String sql, Object... paras) {
    return MAIN.delete(sql, paras);
  }

  /**
   * @see #delete(String, Object...)
   * @param sql an SQL statement
   */
  public static int delete(String sql) {
    return MAIN.delete(sql);
  }

  static <T> List<T> query(Config config, Connection conn, String sql, Object... paras) throws SQLException {
    if (replicas != null) {
      return useReplica().query(config, conn, sql, paras);
    } else {
      return MAIN.query(config, conn, sql, paras);
    }
  }

  /**
   * @see #query(String, String, Object...)
   */
  public static <T> List<T> query(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().query(sql, paras);
    }

    return MAIN.query(sql, paras);
  }

  /**
   * @see #query(String, Object...)
   * @param sql an SQL statement
   */
  public static <T> List<T> query(String sql) {
    if (replicas != null) {
      return useReplica().query(sql);
    }

    return MAIN.query(sql);
  }

  /**
   * @param <T>
   * @param sql
   * @return
   */
  public static <T> List<T> query(SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().query(sqlPara);
    }

    return MAIN.query(sqlPara);
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
  public static <T> T queryFirst(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryFirst(sql, paras);
    }
    return MAIN.queryFirst(sql, paras);
  }

  public static byte[] quereyBytes(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryBytes(sql, paras);
    }
    return MAIN.queryBytes(sql, paras);
  }

  /**
   * @see #queryFirst(String, Object...)
   * @param sql an SQL statement
   */
  public static <T> T queryFirst(String sql) {
    if (replicas != null) {
      return useReplica().queryFirst(sql);
    }
    return MAIN.queryFirst(sql);
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
  public static <T> T queryColumn(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryFirst(sql, paras);
    }
    return MAIN.queryColumn(sql, paras);
  }

  public static <T> T queryColumn(String sql) {
    if (replicas != null) {
      return useReplica().queryColumn(sql);
    }
    return MAIN.queryColumn(sql);
  }

  public static String queryStr(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryStr(sql, paras);
    }
    return MAIN.queryStr(sql, paras);
  }

  public static String queryStr(String sql) {
    if (replicas != null) {
      return useReplica().queryStr(sql);
    }
    return MAIN.queryStr(sql);
  }

  public static Integer queryInt(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryInt(sql, paras);
    }
    return MAIN.queryInt(sql, paras);
  }

  public static <T> T queryColumnById(String tableName, String column, Object id) {
    if (replicas != null) {
      return useReplica().queryColumnById(tableName, column, id);
    }
    return MAIN.queryColumnById(tableName, column, id);
  }

  public static Long queryLongById(String tableName, String column, Object id) {
    if (replicas != null) {
      return useReplica().queryColumnById(tableName, column, id);
    }
    return MAIN.queryColumnById(tableName, column, id);
  }

  public static Long queryStrById(String tableName, String column, Object id) {
    if (replicas != null) {
      return useReplica().queryColumnById(tableName, column, id);
    }
    return MAIN.queryColumnById(tableName, column, id);
  }

  public static <T> T queryColumnByField(String tableName, String column, String field, Object value) {

    if (replicas != null) {
      return useReplica().queryColumnByField(tableName, column, field, value);
    }
    return MAIN.queryColumnByField(tableName, column, field, value);
  }

  public static Integer queryInt(String sql) {
    if (replicas != null) {
      return useReplica().queryInt(sql);
    }
    return MAIN.queryInt(sql);
  }

  public static Long queryLong(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryLong(sql, paras);
    }
    return MAIN.queryLong(sql, paras);
  }

  public static Long queryLong(String sql) {
    if (replicas != null) {
      return useReplica().queryLong(sql);
    }
    return MAIN.queryLong(sql);
  }

  public static Double queryDouble(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryDouble(sql, paras);
    }
    return MAIN.queryDouble(sql, paras);
  }

  public static Double queryDouble(String sql) {
    if (replicas != null) {
      return useReplica().queryDouble(sql);
    }
    return MAIN.queryDouble(sql);
  }

  public static Float queryFloat(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryFloat(sql, paras);
    }
    return MAIN.queryFloat(sql, paras);
  }

  public static Float queryFloat(String sql) {
    if (replicas != null) {
      return useReplica().queryFloat(sql);
    }
    return MAIN.queryFloat(sql);
  }

  public static BigDecimal queryBigDecimal(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryBigDecimal(sql, paras);
    }
    return MAIN.queryBigDecimal(sql, paras);
  }

  public static BigDecimal queryBigDecimal(String sql) {
    if (replicas != null) {
      return useReplica().queryBigDecimal(sql);
    }
    return MAIN.queryBigDecimal(sql);
  }

  public static BigInteger queryBigInteger(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryBigInteger(sql, paras);
    }
    return MAIN.queryBigInteger(sql, paras);
  }

  public static BigInteger queryBigInteger(String sql) {
    if (replicas != null) {
      return useReplica().queryBigInteger(sql);
    }
    return MAIN.queryBigInteger(sql);
  }

  public static byte[] queryBytes(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryBytes(sql, paras);
    }
    return MAIN.queryBytes(sql, paras);
  }

  public static byte[] queryBytes(String sql) {
    if (replicas != null) {
      return useReplica().queryBytes(sql);
    }
    return MAIN.queryBytes(sql);
  }

  public static java.util.Date queryDate(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryDate(sql, paras);
    }
    return MAIN.queryDate(sql, paras);
  }

  public static java.util.Date queryDate(String sql) {
    if (replicas != null) {
      return useReplica().queryDate(sql);
    }
    return MAIN.queryDate(sql);
  }

  public static LocalDateTime queryLocalDateTime(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryLocalDateTime(sql, paras);
    }
    return MAIN.queryLocalDateTime(sql, paras);
  }

  public static LocalDateTime queryLocalDateTime(String sql) {
    if (replicas != null) {
      return useReplica().queryLocalDateTime(sql);
    }
    return MAIN.queryLocalDateTime(sql);
  }

  public static java.sql.Time queryTime(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryTime(sql, paras);
    }
    return MAIN.queryTime(sql, paras);
  }

  public static java.sql.Time queryTime(String sql) {
    if (replicas != null) {
      return useReplica().queryTime(sql);
    }
    return MAIN.queryTime(sql);
  }

  public static java.sql.Timestamp queryTimestamp(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryFirst(sql, paras);
    }
    return MAIN.queryTimestamp(sql, paras);
  }

  public static java.sql.Timestamp queryTimestamp(String sql) {
    if (replicas != null) {
      return useReplica().queryTimestamp(sql);
    }
    return MAIN.queryTimestamp(sql);
  }

  public static Boolean queryBoolean(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryBoolean(sql, paras);
    }
    return MAIN.queryBoolean(sql, paras);
  }

  public static Boolean queryBoolean(String sql) {
    if (replicas != null) {
      return useReplica().queryBoolean(sql);
    }
    return MAIN.queryBoolean(sql);
  }

  public static Short queryShort(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryShort(sql, paras);
    }
    return MAIN.queryShort(sql, paras);
  }

  public static Short queryShort(String sql) {
    if (replicas != null) {
      return useReplica().queryShort(sql);
    }
    return MAIN.queryShort(sql);
  }

  public static Byte queryByte(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryByte(sql, paras);
    }
    return MAIN.queryByte(sql, paras);
  }

  public static Byte queryByte(String sql) {
    if (replicas != null) {
      return useReplica().queryByte(sql);
    }
    return MAIN.queryByte(sql);
  }

  public static Number queryNumber(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryNumber(sql, paras);
    }
    return MAIN.queryNumber(sql, paras);
  }

  public static Number queryNumber(String sql) {
    if (replicas != null) {
      return useReplica().queryNumber(sql);
    }
    return MAIN.queryNumber(sql);
  }

  // ===============================find
  // start===========================================
  /**
   * 
   * @param config
   * @param conn
   * @param sql
   * @param paras
   * @return
   * @throws SQLException
   */
  static List<Row> find(Config config, Connection conn, String sql, Object... paras) throws SQLException {
    if (replicas != null) {
      return useReplica().find(config, conn, sql, paras);
    }
    return MAIN.find(config, conn, sql, paras);
  }

  /**
   * @param sql the sql statement
   * @return
   */
  public static List<Row> find(String sql) {
    if (replicas != null) {
      return useReplica().find(sql);
    }
    return MAIN.find(sql);
  }

  /**
   * @param <T>
   * @param clazz
   * @param sql
   * @return
   */
  public static <T> List<T> find(Class<T> clazz, String sql) {
    if (replicas != null) {
      return useReplica().find(clazz, sql);
    }
    return MAIN.find(clazz, sql);
  }

  /**
   * 
   * @param sql
   * @param paras
   * @return
   */
  public static List<Row> find(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().find(sql, paras);
    }
    return MAIN.find(sql, paras);
  }

  public static List<Row> find(String tableName, Row record) {
    if (replicas != null) {
      return useReplica().find(tableName, record);
    }
    return MAIN.find(tableName, record);
  }

  public static List<Row> findByField(String tableName, String field, Object fieldValue) {
    if (replicas != null) {
      return useReplica().findByField(tableName, field, fieldValue);
    }
    return MAIN.findByField(tableName, field, fieldValue);
  }

  public static List<Row> find(String tableName, String columns, Row record) {
    if (replicas != null) {
      return useReplica().find(tableName, columns, record);
    }
    return MAIN.find(tableName, columns, record);
  }

  public static List<Row> findWithJsonField(String sql, String[] jsonFields, Object... paras) {
    if (replicas != null) {
      return useReplica().findWithJsonField(sql, jsonFields, paras);
    }
    return MAIN.findWithJsonField(sql, jsonFields, paras);
  }

  /**
   * @param <T>
   * @param clazz
   * @param sql
   * @param paras
   * @return
   */
  public static <T> List<T> find(Class<T> clazz, String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().find(clazz, sql, paras);
    }
    return MAIN.find(clazz, sql, paras);
  }

  public static List<Row> findIn(String tableName, String primayKey, Object... paras) {
    if (replicas != null) {
      return useReplica().findIn(tableName, primayKey, paras);
    }
    return MAIN.findIn(tableName, primayKey, paras);
  }

  public static List<Row> findColumnsIn(String tableName, String columns, String primayKey, Object... paras) {
    if (replicas != null) {
      return useReplica().findColumnsIn(tableName, columns, primayKey, paras);
    }
    return MAIN.findColumnsIn(tableName, columns, primayKey, paras);
  }

  public static List<Row> findColumnsIn(String tableName, String columns, String primayKey, List paras) {
    if (replicas != null) {
      return useReplica().findColumnsIn(tableName, columns, primayKey, paras);
    }
    return MAIN.findColumnsIn(tableName, columns, primayKey, paras);
  }

  public static <T> List<T> findAll(Class<T> clazz) {
    String tableName = DbTableNameUtils.getTableName(clazz);
    return findAll(clazz, tableName);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param tableName
   * @return
   */
  public static <T> List<T> findAll(Class<T> clazz, String tableName) {
    if (replicas != null) {
      return useReplica().findAll(clazz, tableName);
    }
    return MAIN.findAll(clazz, tableName);
  }

  /**
   * @param tableName
   * @return
   */
  public static List<Row> findAll(String tableName) {
    if (replicas != null) {
      return useReplica().findAll(tableName);
    }
    return MAIN.findAll(tableName);
  }

  /**
   * 
   * @param tableName
   * @param columns
   * @return
   */
  public static List<Row> findColumns(String tableName, String columns) {
    if (replicas != null) {
      return useReplica().findColumnsAll(tableName, columns);
    }
    return MAIN.findColumnsAll(tableName, columns);
  }

  /**
   * @param <T>
   * @param clazz
   * @param tableName
   * @param columns
   * @return
   */
  public static <T> List<T> findColumns(Class<T> clazz, String tableName, String columns) {
    if (replicas != null) {
      return useReplica().findColumnsAll(clazz, tableName, columns);
    }
    return MAIN.findColumnsAll(clazz, tableName, columns);
  }

  /**
   * Find first record. I recommend add "limit 1" in your sql.
   * 
   * @param sql   an SQL statement that may contain one or more '?' IN parameter
   *              placeholders
   * @param paras the parameters of sql
   * @return the Record object
   */
  public static Row findFirst(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().findFirst(sql, paras);
    }
    return MAIN.findFirst(sql, paras);
  }

  public static Row findFirst(String tableName, Row record) {
    if (replicas != null) {
      return useReplica().findFirst(tableName, record);
    }
    return MAIN.findFirst(tableName, record);
  }

  public static Row findFirst(String tableName, String columns, Row record) {
    if (replicas != null) {
      return useReplica().findFirst(tableName, columns, record);
    }
    return MAIN.findFirst(tableName, columns, record);
  }

  /**
   * @param <T>
   * @param clazz
   * @param sql
   * @param paras
   * @return
   */
  public static <T> T findFirst(Class<T> clazz, String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().findFirst(clazz, sql, paras);
    }
    return MAIN.findFirst(clazz, sql, paras);
  }

  /**
   * @see #findFirst(String, Object...)
   * @param sql an SQL statement
   */
  public static Row findFirst(String sql) {
    if (replicas != null) {
      return useReplica().findFirst(sql);
    }
    return MAIN.findFirst(sql);
  }

  /**
   * @param <T>
   * @param clazz
   * @param sql
   * @return
   */
  public static <T> T findFirst(Class<T> clazz, String sql) {
    if (replicas != null) {
      return useReplica().findFirst(clazz, sql);
    }
    return MAIN.findFirst(clazz, sql);
  }

  /**
   * Find record by id with default primary key.
   * 
   * <pre>
   * Example:
   * Record user = Db.findById("user", 15);
   * </pre>
   * 
   * @param tableName the table name of the table
   * @param idValue   the id value of the record
   */
  public static Row findById(String tableName, Object idValue) {
    if (replicas != null) {
      return useReplica().findById(tableName, idValue);
    }
    return MAIN.findById(tableName, idValue);
  }
  

  public static <T> T findById(Class<T> clazz, Object idValue) {
    if (replicas != null) {
      return useReplica().findById(clazz, idValue);
    }
    return MAIN.findById(clazz, idValue); 
  }

  /**
   * @param <T>
   * @param clazz
   * @param tableName
   * @param idValue
   * @return T
   */
  public static <T> T findById(Class<T> clazz, String tableName, Object idValue) {
    if (replicas != null) {
      return useReplica().findById(clazz, tableName, idValue);
    }
    return MAIN.findById(clazz, tableName, idValue);
  }

  /**
   * @param <T>
   * @param clazz
   * @param tableName
   * @param columns
   * @param idValue
   * @return T
   */
  public static <T> T findColumnsById(Class<T> clazz, String tableName, String columns, Object idValue) {
    if (replicas != null) {
      return useReplica().findColumnsById(clazz, tableName, columns, idValue);
    }
    return MAIN.findColumnsById(clazz, tableName, columns, idValue);
  }

  /**
   * @param tableName
   * @param columns
   * @param idValue
   * @return Record
   */
  public static Row findColumnsById(String tableName, String columns, Object idValue) {
    if (replicas != null) {
      return useReplica().findColumnsById(tableName, columns, idValue);
    }
    return MAIN.findColumnsById(tableName, columns, idValue);
  }

  /**
   * 
   * @param tableName
   * @param columns
   * @param primaryKey
   * @param idValue
   * @return Record
   */
  public static Row findColumnsById(String tableName, String columns, String primaryKey, Object idValue) {
    if (replicas != null) {
      return useReplica().findColumnsById(tableName, columns, primaryKey, idValue);
    }
    return MAIN.findColumnsById(tableName, columns, primaryKey, idValue);
  }

  /**
   * @param tableName
   * @param primaryKey
   * @param idValue
   * @return
   */
  public static Row findById(String tableName, String primaryKey, Object idValue) {
    if (replicas != null) {
      return useReplica().findById(tableName, primaryKey, idValue);
    }
    return MAIN.findById(tableName, primaryKey, idValue);
  }

  /**
   * @param <T>
   * @param clazz
   * @param tableName
   * @param primaryKey
   * @param idValue
   * @return
   */
  public static <T> T findById(Class<T> clazz, String tableName, String primaryKey, Object idValue) {
    if (replicas != null) {
      return useReplica().findById(clazz, tableName, primaryKey, idValue);
    }
    return MAIN.findById(clazz, tableName, primaryKey, idValue);
  }

  /**
   * Find record by ids.
   * 
   * <pre>
   * Example:
   * Record user = Db.findByIds("user", "user_id", 123);
   * Record userRole = Db.findByIds("user_role", "user_id, role_id", 123, 456);
   * </pre>
   * 
   * @param tableName  the table name of the table
   * @param primaryKey the primary key of the table, composite primary key is
   *                   separated by comma character: ","
   * @param idValues   the id value of the record, it can be composite id values
   */
  public static Row findByIds(String tableName, String primaryKey, Object... idValues) {
    if (replicas != null) {
      return useReplica().findByIds(tableName, primaryKey, idValues);
    }
    return MAIN.findByIds(tableName, primaryKey, idValues);
  }

  public static <T> T findByIds(Class<T> clazz, String tableName, String primaryKey, Object... idValues) {
    if (replicas != null) {
      return useReplica().findByIds(clazz, tableName, primaryKey, idValues);
    }
    return MAIN.findByIds(clazz, tableName, primaryKey, idValues);
  }

  /**
   * @param tableName
   * @param columns
   * @param primaryKey
   * @param idValues
   * @return
   */
  public static Row findColumnsByIds(String tableName, String columns, String primaryKey, Object... idValues) {
    if (replicas != null) {
      return useReplica().findColumnsByIds(tableName, columns, primaryKey, idValues);
    }
    return MAIN.findColumnsByIds(tableName, columns, primaryKey, idValues);
  }

  /**
   * @param <T>
   * @param clazz
   * @param tableName
   * @param columns
   * @param primaryKey
   * @param idValues
   * @return
   */
  public static <T> T findColumnsByIds(Class<T> clazz, String tableName, String columns, String primaryKey,
      Object... idValues) {
    if (replicas != null) {
      return useReplica().findColumnsByIds(clazz, tableName, columns, primaryKey, idValues);
    }
    return MAIN.findColumnsByIds(clazz, tableName, columns, primaryKey, idValues);
  }

  public static List<Row> findByColumn(String tableName, String column, Object value) {
    if (replicas != null) {
      return useReplica().findByColumn(tableName, column, value);
    }
    return MAIN.findByColumn(tableName, column, value);
  }

  // ===========================================paginate
  /**
   * @param config
   * @param conn
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   * @throws SQLException
   */
  static Page<Row> paginate(Config config, Connection conn, int pageNumber, int pageSize, String select,
      String sqlExceptSelect, Object... paras) throws SQLException {
    if (replicas != null) {
      return useReplica().paginate(config, conn, pageNumber, pageSize, select, sqlExceptSelect, paras);
    }
    return MAIN.paginate(config, conn, pageNumber, pageSize, select, sqlExceptSelect, paras);
  }

  /**
   * 
   * @param pageNumber
   * @param pageSize
   * @param sqlPara
   * @return
   */
  public static Page<Row> paginate(int pageNumber, int pageSize, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginate(pageNumber, pageSize, sqlPara);
    }
    return MAIN.paginate(pageNumber, pageSize, sqlPara);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param sqlPara
   * @return
   */
  public static Page<Row> paginate(int pageNumber, int pageSize, boolean isGroupBySql, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginate(pageNumber, pageSize, isGroupBySql, sqlPara);
    }
    return MAIN.paginate(pageNumber, pageSize, isGroupBySql, sqlPara);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static Page<Row> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginate(pageNumber, pageSize, select, sqlExceptSelect);
    }
    return MAIN.paginate(pageNumber, pageSize, select, sqlExceptSelect);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static Page<Row> paginate(int pageNumber, int pageSize, boolean isGroupBySql, String select,
      String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginate(pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect);
    }
    return MAIN.paginate(pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect);
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
  public static Page<Row> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect,
      Object... paras) {
    if (replicas != null) {
      return useReplica().paginate(pageNumber, pageSize, select, sqlExceptSelect, paras);
    }
    return MAIN.paginate(pageNumber, pageSize, select, sqlExceptSelect, paras);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   */
  public static Page<Row> paginate(int pageNumber, int pageSize, boolean isGroupBySql, String select,
      String sqlExceptSelect, Object... paras) {
    if (replicas != null) {
      return useReplica().paginate(pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
    }
    return MAIN.paginate(pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static Page<Row> paginateByFullSql(int pageNumber, int pageSize, String totalRowSql, String findSql,
      Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByFullSql(pageNumber, pageSize, totalRowSql, findSql, paras);
    }
    return MAIN.paginateByFullSql(pageNumber, pageSize, totalRowSql, findSql, paras);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static Page<Row> paginateByFullSql(int pageNumber, int pageSize, boolean isGroupBySql, String totalRowSql,
      String findSql, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByFullSql(pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    }
    return MAIN.paginateByFullSql(pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
  }

  /**
   * @param <T>
   * @param clazz
   * @param pageNumber
   * @param pageSize
   * @param sqlPara
   * @return
   */
  public static <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginate(clazz, pageNumber, pageSize, sqlPara);
    }
    return MAIN.paginate(clazz, pageNumber, pageSize, sqlPara);
  }

  /**
   * @param <T>
   * @param clazz
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param sqlPara
   * @return
   */
  public static <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql,
      SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginate(clazz, pageNumber, pageSize, isGroupBySql, sqlPara);
    }
    return MAIN.paginate(clazz, pageNumber, pageSize, isGroupBySql, sqlPara);
  }

  /**
   * @param <T>
   * @param clazz
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, String select,
      String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginate(clazz, pageNumber, pageSize, select, sqlExceptSelect);
    }
    return MAIN.paginate(clazz, pageNumber, pageSize, select, sqlExceptSelect);
  }

  /**
   * @param <T>
   * @param clazz
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql, String select,
      String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginate(clazz, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect);
    }
    return MAIN.paginate(clazz, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect);
  }

  /**
   * @param <T>
   * @param clazz
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   */
  public static <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, String select,
      String sqlExceptSelect, Object... paras) {
    if (replicas != null) {
      return useReplica().paginate(clazz, pageNumber, pageSize, select, sqlExceptSelect, paras);
    }
    return MAIN.paginate(clazz, pageNumber, pageSize, select, sqlExceptSelect, paras);
  }

  /**
   * @param <T>
   * @param clazz
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   */
  public static <T> Page<T> paginate(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql, String select,
      String sqlExceptSelect, Object... paras) {
    if (replicas != null) {
      return useReplica().paginate(clazz, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
    }
    return MAIN.paginate(clazz, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static <T> Page<T> paginateByFullSql(Class<T> clazz, int pageNumber, int pageSize, String totalRowSql,
      String findSql, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByFullSql(clazz, pageNumber, pageSize, totalRowSql, findSql, paras);
    }
    return MAIN.paginateByFullSql(clazz, pageNumber, pageSize, totalRowSql, findSql, paras);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static <T> Page<T> paginateByFullSql(Class<T> clazz, int pageNumber, int pageSize, boolean isGroupBySql,
      String totalRowSql, String findSql, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByFullSql(clazz, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
    }
    return MAIN.paginateByFullSql(clazz, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql, paras);
  }

  /**
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param sqlPara
   * @return
   */
  public static Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginateByCache(cacheName, key, pageNumber, pageSize, sqlPara);
    }
    return MAIN.paginateByCache(cacheName, key, pageNumber, pageSize, sqlPara);
  }

  /**
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param sqlPara
   * @return
   */
  public static Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize,
      boolean isGroupBySql, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, sqlPara);
    }
    return MAIN.paginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, sqlPara);
  }

  /**
   * Paginate by cache.
   * 
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   */
  public static Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, String select,
      String sqlExceptSelect, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCache(cacheName, key, pageNumber, pageSize, select, sqlExceptSelect, paras);
    }
    return MAIN.paginateByCache(cacheName, key, pageNumber, pageSize, select, sqlExceptSelect, paras);
  }

  /**
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   */
  public static Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize,
      boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect,
          paras);
    }
    return MAIN.paginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect, paras);
  }

  /**
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize, String select,
      String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginateByCache(cacheName, key, pageNumber, pageSize, select, sqlExceptSelect);
    }
    return MAIN.paginateByCache(cacheName, key, pageNumber, pageSize, select, sqlExceptSelect);
  }

  /**
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static Page<Row> paginateByCache(String cacheName, Object key, int pageNumber, int pageSize,
      boolean isGroupBySql, String select, String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect);
    }
    return MAIN.paginateByCache(cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static Page<Row> paginateByCacheByFullSql(String cacheName, Object key, int pageNumber, int pageSize,
      String totalRowSql, String findSql, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCacheByFullSql(cacheName, key, pageNumber, pageSize, totalRowSql, findSql, paras);
    }
    return MAIN.paginateByCacheByFullSql(cacheName, key, pageNumber, pageSize, totalRowSql, findSql, paras);
  }

  /**
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static Page<Row> paginateByCacheByFullSql(String cacheName, Object key, int pageNumber, int pageSize,
      boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCacheByFullSql(cacheName, key, pageNumber, pageSize, isGroupBySql, totalRowSql,
          findSql, paras);
    }
    return MAIN.paginateByCacheByFullSql(cacheName, key, pageNumber, pageSize, isGroupBySql, totalRowSql, findSql,
        paras);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize,
      boolean isGroupBySql, String select, String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, select,
          sqlExceptSelect);
    }
    return MAIN.paginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param sqlPara
   * @return
   */
  public static <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize,
      boolean isGroupBySql, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, sqlPara);
    }
    return MAIN.paginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, sqlPara);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param sqlPara
   * @return
   */
  public static <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize,
      SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().paginateByCache(clazz, cacheName, key, pageNumber, pageSize, sqlPara);
    }
    return MAIN.paginateByCache(clazz, cacheName, key, pageNumber, pageSize, sqlPara);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @return
   */
  public static <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize,
      String select, String sqlExceptSelect) {
    if (replicas != null) {
      return useReplica().paginateByCache(clazz, cacheName, key, pageNumber, pageSize, select, sqlExceptSelect);
    }
    return MAIN.paginateByCache(clazz, cacheName, key, pageNumber, pageSize, select, sqlExceptSelect);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   */
  public static <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize,
      String select, String sqlExceptSelect, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCache(clazz, cacheName, key, pageNumber, pageSize, select, sqlExceptSelect, paras);
    }
    return MAIN.paginateByCache(clazz, cacheName, key, pageNumber, pageSize, select, sqlExceptSelect, paras);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param key
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param select
   * @param sqlExceptSelect
   * @param paras
   * @return
   */
  public static <T> Page<T> paginateByCache(Class<T> clazz, String cacheName, Object key, int pageNumber, int pageSize,
      boolean isGroupBySql, String select, String sqlExceptSelect, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, select,
          sqlExceptSelect, paras);
    }
    return MAIN.paginateByCache(clazz, cacheName, key, pageNumber, pageSize, isGroupBySql, select, sqlExceptSelect,
        paras);
  }

  /**
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param cacheKey
   * @param pageNumber
   * @param pageSize
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static <T> Page<T> paginateByCacheByFullSql(Class<T> clazz, String cacheName, Object cacheKey, int pageNumber,
      int pageSize, String totalRowSql, String findSql, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCacheByFullSql(clazz, cacheName, cacheKey, pageNumber, pageSize, totalRowSql,
          findSql, paras);
    }
    return MAIN.paginateByCacheByFullSql(clazz, cacheName, cacheKey, pageNumber, pageSize, totalRowSql, findSql, paras);
  }

  /**
   * @param <T>
   * @param clazz
   * @param cacheName
   * @param cacheKey
   * @param pageNumber
   * @param pageSize
   * @param isGroupBySql
   * @param totalRowSql
   * @param findSql
   * @param paras
   * @return
   */
  public static <T> Page<T> paginateByCacheByFullSql(Class<T> clazz, String cacheName, Object cacheKey, int pageNumber,
      int pageSize, boolean isGroupBySql, String totalRowSql, String findSql, Object... paras) {
    if (replicas != null) {
      return useReplica().paginateByCacheByFullSql(clazz, cacheName, cacheKey, pageNumber, pageSize, isGroupBySql,
          totalRowSql, findSql, paras);
    }
    return MAIN.paginateByCacheByFullSql(clazz, cacheName, cacheKey, pageNumber, pageSize, isGroupBySql, totalRowSql,
        findSql, paras);
  }

  /**
   * @see #execute(String, ICallback)
   */
  public static Object execute(ICallback callback) {
    return MAIN.execute(callback);
  }

  /**
   * Execute callback. It is useful when all the API can not satisfy your
   * requirement.
   * 
   * @param config   the Config object
   * @param callback the ICallback interface
   */
  static Object execute(Config config, ICallback callback) {
    return MAIN.execute(config, callback);
  }

  /**
   * Execute transaction.
   * 
   * @param config           the Config object
   * @param transactionLevel the transaction level
   * @param atom             the atom operation
   * @return true if transaction executing succeed otherwise false
   */
  static boolean tx(Config config, int transactionLevel, IAtom atom) {
    return MAIN.tx(config, transactionLevel, atom);
  }

  /**
   * Execute transaction with default transaction level.
   * 
   * @see #tx(int, IAtom)
   */
  public static boolean tx(IAtom atom) {
    return MAIN.tx(atom);
  }

  public static boolean tx(int transactionLevel, IAtom atom) {
    return MAIN.tx(transactionLevel, atom);
  }

  /**
   * 主要用于嵌套事务场景
   * 
   * 实例：https://jfinal.com/feedback/4008
   * 
   * 默认情况下嵌套事务会被合并成为一个事务，那么内层与外层任何地方回滚事务 所有嵌套层都将回滚事务，也就是说嵌套事务无法独立提交与回滚
   * 
   * 使用 txInNewThread(...) 方法可以实现层之间的事务控制的独立性 由于事务处理是将 Connection 绑定到线程上的，所以
   * txInNewThread(...) 通过建立新线程来实现嵌套事务的独立控制
   */
  public static Future<Boolean> txInNewThread(IAtom atom) {
    return MAIN.txInNewThread(atom);
  }

  public static Future<Boolean> txInNewThread(int transactionLevel, IAtom atom) {
    return MAIN.txInNewThread(transactionLevel, atom);
  }

  /**
   * Find Record by cache.
   * 
   * @see #find(String, Object...)
   * @param cacheName the cache name
   * @param key       the key used to get date from cache
   * @return the list of Record
   */
  public static List<Row> findByCache(String cacheName, Object key, String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().findByCache(cacheName, key, sql, paras);
    }
    return MAIN.findByCache(cacheName, key, sql, paras);
  }

  public static <T> List<T> findByCache(Class<T> clazz, String cacheName, Object key, String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().findByCache(clazz, cacheName, key, sql, paras);
    }
    return MAIN.findByCache(clazz, cacheName, key, sql, paras);
  }

  /**
   * @see #findByCache(String, Object, String, Object...)
   */
  public static List<Row> findByCache(String cacheName, Object key, String sql) {
    if (replicas != null) {
      return useReplica().findByCache(cacheName, key, sql);
    }
    return MAIN.findByCache(cacheName, key, sql);
  }

  public static <T> List<T> findByCache(Class<T> clazz, String cacheName, Object key, String sql) {
    if (replicas != null) {
      return useReplica().findByCache(clazz, cacheName, key, sql);
    }
    return MAIN.findByCache(clazz, cacheName, key, sql);
  }

  /**
   * Find first record by cache. I recommend add "limit 1" in your sql.
   * 
   * @see #findFirst(String, Object...)
   * @param cacheName the cache name
   * @param key       the key used to get date from cache
   * @param sql       an SQL statement that may contain one or more '?' IN
   *                  parameter placeholders
   * @param paras     the parameters of sql
   * @return the Record object
   */
  public static Row findFirstByCache(String cacheName, Object key, String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().findFirstByCache(cacheName, key, sql, paras);
    }
    return MAIN.findFirstByCache(cacheName, key, sql, paras);
  }

  public static Row findFirstByCache(String cacheName, Object key, int ttl, String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().findFirstByCache(cacheName, key, ttl, sql, paras);
    }
    return MAIN.findFirstByCache(cacheName, key, ttl, sql, paras);
  }

  public static <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().findFirstByCache(clazz, cacheName, key, sql, paras);
    }
    return MAIN.findFirstByCache(clazz, cacheName, key, sql, paras);
  }

  /**
   * @see #findFirstByCache(String, Object, String, Object...)
   */
  public static Row findFirstByCache(String cacheName, Object key, String sql) {
    if (replicas != null) {
      return useReplica().findFirstByCache(cacheName, key, sql);
    }
    return MAIN.findFirstByCache(cacheName, key, sql);
  }

  public static Row findFirstByCache(String cacheName, Object key, int ttl, String sql) {
    if (replicas != null) {
      return useReplica().findFirstByCache(cacheName, key, ttl, sql);
    }
    return MAIN.findFirstByCache(cacheName, key, ttl, sql);
  }

  public static <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, String sql) {
    if (replicas != null) {
      return useReplica().findFirstByCache(clazz, cacheName, key, sql);
    }
    return MAIN.findFirstByCache(clazz, cacheName, key, sql);
  }

  public static <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, int ttl, String sql) {
    if (replicas != null) {
      return useReplica().findFirstByCache(clazz, cacheName, key, ttl, sql);
    }
    return MAIN.findFirstByCache(clazz, cacheName, key, ttl, sql);
  }

  public static <T> T findFirstByCache(Class<T> clazz, String cacheName, Object key, int ttl, String sql,
      Object... paras) {
    if (replicas != null) {
      return useReplica().findFirstByCache(clazz, cacheName, key, ttl, sql, paras);
    }
    return MAIN.findFirstByCache(clazz, cacheName, key, ttl, sql, paras);
  }

  /**
   * @see DbPro#batch(String, Object[][], int)
   */
  public static int[] batch(String sql, Object[][] paras, int batchSize) {
    return MAIN.batch(sql, paras, batchSize);
  }

  /**
   * @see DbPro#batch(String, String, List, int)
   */
  public static int[] batch(String sql, String columns, List modelOrRecordList, int batchSize) {
    return MAIN.batch(sql, columns, modelOrRecordList, batchSize);
  }

  /**
   * @see DbPro#batch(List, int)
   */
  public static int[] batch(List<String> sqlList, int batchSize) {
    return MAIN.batch(sqlList, batchSize);
  }

  /**
   * @see DbPro#batchSave(List, int)
   */
  public static int[] batchSave(List<? extends Model> modelList, int batchSize) {
    return MAIN.batchSave(modelList, batchSize);
  }

  /**
   * @see DbPro#batchSave(String, List, int)
   */
  public static int[] batchSave(String tableName, List<? extends Row> recordList, int batchSize) {
    return MAIN.batchSave(tableName, recordList, batchSize);
  }

  public static int[] batchSave(String tableName, String[] jsonFields, List<Row> recordList, int batchSize) {
    return MAIN.batchSave(tableName, jsonFields, recordList, batchSize);
  }

  public static int[] batchDelete(String tableName, List<? extends Row> recordList, int batchSize) {
    return MAIN.batchDelete(tableName, recordList, batchSize);
  }

  /**
   * @see DbPro#batchUpdate(List, int)
   */
  public static int[] batchUpdate(List<? extends Model> modelList, int batchSize) {
    return MAIN.batchUpdate(modelList, batchSize);
  }

  /**
   * @see DbPro#batchUpdate(String, String, List, int)
   */
  public static int[] batchUpdate(String tableName, String primaryKey, List<? extends Row> recordList, int batchSize) {
    return MAIN.batchUpdate(tableName, primaryKey, recordList, batchSize);
  }

  /**
   * @see DbPro#batchUpdate(String, List, int)
   */
  public static int[] batchUpdate(String tableName, List<? extends Row> recordList, int batchSize) {
    return MAIN.batchUpdate(tableName, recordList, batchSize);
  }

  public static String getSql(String key) {
    return MAIN.getSql(key);
  }

  // 支持传入变量用于 sql 生成。为了避免用户将参数拼接在 sql 中引起 sql 注入风险，只在 SqlKit 中开放该功能
  // public static String getSql(String key, Map data) {
  // return MAIN.getSql(key, data);
  // }

  public static SqlPara getSqlPara(String key, Row record) {
    return MAIN.getSqlPara(key, record);
  }

  public static SqlPara getSqlPara(String key, Model model) {
    return MAIN.getSqlPara(key, model);
  }

  public static SqlPara getSqlPara(String key, Map data) {
    return MAIN.getSqlPara(key, data);
  }

  public static SqlPara getSqlPara(String key, Object... paras) {
    return MAIN.getSqlPara(key, paras);
  }

  public static SqlPara getSqlParaByString(String content, Map data) {
    return MAIN.getSqlParaByString(content, data);
  }

  public static SqlPara getSqlParaByString(String content, Object... paras) {
    return MAIN.getSqlParaByString(content, paras);
  }

  // ===================find start========================================
  /**
   * @param sqlPara
   * @return
   */
  public static List<Row> find(SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().find(sqlPara);
    }
    return MAIN.find(sqlPara);
  }

  /**
   * @param <T>
   * @param clazz
   * @param sqlPara
   * @return
   */
  public static <T> List<T> find(Class<T> clazz, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().find(clazz, sqlPara);
    }
    return MAIN.find(clazz, sqlPara);
  }

  /**
   * 
   * @param sqlPara
   * @return
   */
  public static Row findFirst(SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().findFirst(sqlPara);
    }
    return MAIN.findFirst(sqlPara);
  }

  /**
   * 
   * @param <T>
   * @param clazz
   * @param sqlPara
   * @return
   */
  public static <T> T findFirst(Class<T> clazz, SqlPara sqlPara) {
    if (replicas != null) {
      return useReplica().findFirst(clazz, sqlPara);
    }
    return MAIN.findFirst(clazz, sqlPara);
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
  public static void each(Function<Row, Boolean> func, String sql, Object... paras) {
    if (replicas != null) {
      useReplica().each(func, sql, paras);
    }
    MAIN.each(func, sql, paras);
  }

  // ---------

  /**
   * 使用 sql 模板进行查询，可以省去 Db.getSqlPara(...) 调用
   * 
   * <pre>
   * 例子：
   * Db.template("blog.find", Kv.by("id", 123).find();
   * </pre>
   */
  public static DbTemplate template(String key, Map data) {
    if (replicas != null) {
      return useReplica().template(key, data);
    }
    return MAIN.template(key, data);
  }

  /**
   * 使用 sql 模板进行查询，可以省去 Db.getSqlPara(...) 调用
   * 
   * <pre>
   * 例子：
   * Db.template("blog.find", 123).find();
   * </pre>
   */
  public static DbTemplate template(String key, Object... paras) {
    if (replicas != null) {
      return useReplica().template(key, paras);
    }
    return MAIN.template(key, paras);
  }

  // ---------

  /**
   * 使用字符串变量作为 sql 模板进行查询，可省去外部 sql 文件来使用 sql 模板功能
   * 
   * <pre>
   * 例子：
   * String sql = "select * from blog where id = #para(id)";
   * Db.templateByString(sql, Kv.by("id", 123).find();
   * </pre>
   */
  public static DbTemplate templateByString(String content, Map data) {
    if (replicas != null) {
      return useReplica().templateByString(content, data);
    }
    return MAIN.templateByString(content, data);
  }

  /**
   * 使用字符串变量作为 sql 模板进行查询，可省去外部 sql 文件来使用 sql 模板功能
   * 
   * <pre>
   * 例子：
   * String sql = "select * from blog where id = #para(0)";
   * Db.templateByString(sql, 123).find();
   * </pre>
   */
  public static DbTemplate templateByString(String content, Object... paras) {
    if (replicas != null) {
      return useReplica().templateByString(content, paras);
    }
    return MAIN.templateByString(content, paras);
  }

  public static boolean existsBySql(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().exists(sql, paras);
    }
    return MAIN.exists(sql, paras);
  }

  /**
   * @param tableName
   * @param fields
   * @param paras
   * @return
   */
  public static boolean exists(String tableName, String fields, Object... paras) {
    if (replicas != null) {
      return useReplica().exists(tableName, fields, paras);
    }
    return MAIN.exists(tableName, fields, paras);
  }

  public static Long count(String sql) {
    if (replicas != null) {
      return useReplica().count(sql);
    }
    return MAIN.count(sql);
  }

  public static Long countTable(String table) {
    if (replicas != null) {
      return useReplica().countTable(table);
    }
    return MAIN.countTable(table);
  }

  public Long countBySql(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryLong(sql, paras);
    }
    return MAIN.queryLong(sql, paras);
  }

  public static List<String> queryListString(String sql) {
    if (replicas != null) {
      return useReplica().query(sql);
    }
    return MAIN.query(sql);
  }

  public static List<String> queryListString(String sql, Object... params) {
    if (replicas != null) {
      return useReplica().query(sql, params);
    }
    return MAIN.query(sql, params);
  }

  public static List<Integer> queryListInteger(String sql) {
    if (replicas != null) {
      return useReplica().query(sql);
    }
    return MAIN.query(sql);
  }

  public static List<Integer> queryListInteger(String sql, Object... params) {
    if (replicas != null) {
      return useReplica().query(sql, params);
    }
    return MAIN.query(sql, params);
  }

  public static List<Long> queryListLong(String sql) {
    if (replicas != null) {
      return useReplica().query(sql);
    }
    return MAIN.query(sql);
  }

  public static List<Long> queryListLong(String sql, Object... params) {
    if (replicas != null) {
      return useReplica().query(sql, params);
    }
    return MAIN.query(sql, params);
  }

  public static PGobject queryPGobjectById(String tableName, String column, Object id) {
    return queryColumnById(tableName, column, id);
  }

  public static PGobject queryPGobject(String sql, Object... paras) {
    if (replicas != null) {
      return useReplica().queryPGobject(sql, paras);
    }
    return MAIN.queryPGobject(sql, paras);
  }


}