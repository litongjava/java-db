package com.litongjava.db.activerecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import com.jfinal.kit.StrKit;
import com.litongjava.cache.ICache;
import com.litongjava.db.activerecord.bean.DefaultRecordConvert;
import com.litongjava.db.activerecord.cache.DefaultEhCache;
import com.litongjava.db.activerecord.dialect.Dialect;
import com.litongjava.db.activerecord.dialect.MysqlDialect;
import com.litongjava.db.activerecord.sql.SqlKit;
import com.litongjava.db.activerecord.stat.ISqlStatementStat;
import com.litongjava.record.RecordConvert;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Config {
  private final ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();

  String name;
  DataSource dataSource;

  Dialect dialect;
  boolean showSql;
  boolean devMode;
  int transactionLevel;
  IContainerFactory containerFactory;
  IDbProFactory dbProFactory = IDbProFactory.defaultDbProFactory;
  ICache cache;

  SqlKit sqlKit;

  ISqlStatementStat stat;
  boolean writeSync;

  private RecordConvert recordConvert;

  // For ActiveRecordPlugin only, dataSource can be null
  public Config(String name, DataSource dataSource, int transactionLevel) {
    init(name, dataSource, new MysqlDialect(), false, false, transactionLevel, IContainerFactory.defaultContainerFactory, new DefaultEhCache());
  }

  /**
   * Constructor with full parameters
   * 
   * @param name             the name of the config
   * @param dataSource       the dataSource
   * @param dialect          the dialect
   * @param showSql          the showSql
   * @param devMode          the devMode
   * @param transactionLevel the transaction level
   * @param containerFactory the containerFactory
   * @param cache            the cache
   */
  public Config(String name, DataSource dataSource, Dialect dialect, boolean showSql, boolean devMode, int transactionLevel, IContainerFactory containerFactory, ICache cache) {
    if (dataSource == null) {
      throw new IllegalArgumentException("DataSource can not be null");
    }
    init(name, dataSource, dialect, showSql, devMode, transactionLevel, containerFactory, cache);
  }

  private void init(String name, DataSource dataSource, Dialect dialect, boolean showSql, boolean devMode, int transactionLevel, IContainerFactory containerFactory, ICache cache) {
    if (StrKit.isBlank(name)) {
      throw new IllegalArgumentException("Config name can not be blank");
    }
    if (dialect == null) {
      throw new IllegalArgumentException("Dialect can not be null");
    }
    if (containerFactory == null) {
      throw new IllegalArgumentException("ContainerFactory can not be null");
    }
    if (cache == null) {
      throw new IllegalArgumentException("Cache can not be null");
    }

    this.name = name.trim();
    this.dataSource = dataSource;
    this.dialect = dialect;
    this.showSql = showSql;
    this.devMode = devMode;
    // this.transactionLevel = transactionLevel;
    this.setTransactionLevel(transactionLevel);
    this.containerFactory = containerFactory;
    this.cache = cache;

    this.sqlKit = new SqlKit(this.name, this.devMode);
  }

  /**
   * Constructor with name and dataSource
   */
  public Config(String name, DataSource dataSource) {
    this(name, dataSource, new MysqlDialect());
  }

  /**
   * Constructor with name, dataSource and dialect
   */
  public Config(String name, DataSource dataSource, Dialect dialect) {
    this(name, dataSource, dialect, false, false, DbKit.DEFAULT_TRANSACTION_LEVEL, IContainerFactory.defaultContainerFactory, new DefaultEhCache());
  }

  private Config() {

  }

  void setDevMode(boolean devMode) {
    this.devMode = devMode;
    this.sqlKit.setDevMode(devMode);
  }

  void setTransactionLevel(int transactionLevel) {
    int t = transactionLevel;
    if (t != 0 && t != 1 && t != 2 && t != 4 && t != 8) {
      throw new IllegalArgumentException("The transactionLevel only be 0, 1, 2, 4, 8");
    }
    this.transactionLevel = transactionLevel;
  }

  public void setSqlStatementStat(ISqlStatementStat stat, boolean writeSync) {
    this.stat = stat;
    this.writeSync = writeSync;
  }

  public ISqlStatementStat getSqlStatementStat() {
    return stat;
  }

  public boolean writeSync() {
    return writeSync;
  }

  /**
   * Create broken config for DbKit.brokenConfig = Config.createBrokenConfig();
   */
  static Config createBrokenConfig() {
    Config ret = new Config();
    ret.dialect = new MysqlDialect();
    ret.showSql = false;
    ret.devMode = false;
    ret.transactionLevel = DbKit.DEFAULT_TRANSACTION_LEVEL;
    ret.containerFactory = IContainerFactory.defaultContainerFactory;
    ret.cache = new DefaultEhCache();
    return ret;
  }

  public String getName() {
    return name;
  }

  public SqlKit getSqlKit() {
    return sqlKit;
  }

  public Dialect getDialect() {
    return dialect;
  }

  public ICache getCache() {
    return cache;
  }

  public int getTransactionLevel() {
    return transactionLevel;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public IContainerFactory getContainerFactory() {
    return containerFactory;
  }

  public IDbProFactory getDbProFactory() {
    return dbProFactory;
  }

  public boolean isShowSql() {
    return showSql;
  }

  public boolean isDevMode() {
    return devMode;
  }

  // --------

  /**
   * Support transaction with Transaction interceptor
   */
  public void setThreadLocalConnection(Connection connection) {
    threadLocal.set(connection);
  }

  public void removeThreadLocalConnection() {
    threadLocal.remove();
  }

  /**
   * Get Connection. Support transaction if Connection in ThreadLocal
   */
  public Connection getConnection() throws SQLException {
    Connection conn = threadLocal.get();
    if (conn != null) {
      return conn;
    }

    Connection rawConnection = dataSource.getConnection();

    if (showSql) {
      return new SqlReporter(rawConnection).getConnection();
    } else {
      return rawConnection;
    }
  }

  /**
   * Helps to implement nested transaction. Tx.intercept(...) and Db.tx(...) need
   * this method to detected if it in nested transaction.
   */
  public Connection getThreadLocalConnection() {
    return threadLocal.get();
  }

  /**
   * Return true if current thread in transaction.
   */
  public boolean isInTransaction() {
    return threadLocal.get() != null;
  }

  /**
   * Close ResultSet、Statement、Connection ThreadLocal support declare transaction.
   */
  public void close(ResultSet rs, Statement st, Connection conn) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        log.error(e.getMessage(), e);
      }
    }
    if (st != null) {
      try {
        st.close();
      } catch (SQLException e) {
        log.error(e.getMessage(), e);
      }
    }

    if (threadLocal.get() == null) { // in transaction if conn in threadlocal
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e);
        }
      }
    }
  }

  public void close(Statement st, Connection conn) {
    if (st != null) {
      try {
        st.close();
      } catch (SQLException e) {
        log.error(e.getMessage(), e);
      }
    }

    if (threadLocal.get() == null) { // in transaction if conn in threadlocal
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e);
        }
      }
    }
  }

  public void close(Connection conn) {
    if (threadLocal.get() == null) // in transaction if conn in threadlocal
      if (conn != null)
        try {
          conn.close();
        } catch (SQLException e) {
          throw new ActiveRecordException(e);
        }
  }

  public RecordConvert getRecordConvert() {
    if (recordConvert == null) {
      synchronized (name) {
        if (recordConvert == null) {
          recordConvert = new DefaultRecordConvert();
        }
      }
    }
    return recordConvert;
  }

  public void setRecordConvert(RecordConvert convert) {
    this.recordConvert = convert;
  }
}
