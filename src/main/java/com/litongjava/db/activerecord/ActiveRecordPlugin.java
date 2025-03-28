package com.litongjava.db.activerecord;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import com.jfinal.kit.StrKit;
import com.litongjava.cache.IDbCache;
import com.litongjava.db.activerecord.cache.DefaultEhCache;
import com.litongjava.db.activerecord.dialect.Dialect;
import com.litongjava.db.activerecord.dialect.MysqlDialect;
import com.litongjava.db.activerecord.sql.SqlKit;
import com.litongjava.db.activerecord.stat.ISqlStatementStat;
import com.litongjava.plugin.IPlugin;

import lombok.extern.slf4j.Slf4j;

/**
 * ActiveRecord plugin. <br>
 * ActiveRecord plugin not support mysql type year, you can use int instead of
 * year. Mysql error message for type year when insert a record: Data truncated
 * for column 'xxx' at row 1
 */
@Slf4j
public class ActiveRecordPlugin implements IPlugin {

  protected TableBuilder tableBuilder = new TableBuilder();

  protected IDataSourceProvider dataSourceProvider = null;
  protected Boolean devMode = null;

  protected Config config = null;

  protected volatile boolean isStarted = false;
  protected List<Table> tableList = new ArrayList<Table>();

  public ActiveRecordPlugin(String configName, DataSource dataSource, int transactionLevel) {
    if (StrKit.isBlank(configName)) {
      throw new IllegalArgumentException("configName can not be blank");
    }
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource can not be null");
    }
    this.config = new Config(configName, dataSource, transactionLevel);
  }

  public ActiveRecordPlugin(DataSource dataSource) {
    this(DbKit.MAIN_CONFIG_NAME, dataSource);
  }

  public ActiveRecordPlugin(String configName, DataSource dataSource) {
    this(configName, dataSource, DbKit.DEFAULT_TRANSACTION_LEVEL);
  }

  public ActiveRecordPlugin(DataSource dataSource, int transactionLevel) {
    this(DbKit.MAIN_CONFIG_NAME, dataSource, transactionLevel);
  }

  public ActiveRecordPlugin(String configName, IDataSourceProvider dataSourceProvider, int transactionLevel) {
    if (StrKit.isBlank(configName)) {
      throw new IllegalArgumentException("configName can not be blank");
    }
    if (dataSourceProvider == null) {
      throw new IllegalArgumentException("dataSourceProvider can not be null");
    }
    this.dataSourceProvider = dataSourceProvider;
    this.config = new Config(configName, null, transactionLevel);
  }

  public ActiveRecordPlugin(IDataSourceProvider dataSourceProvider) {
    this(DbKit.MAIN_CONFIG_NAME, dataSourceProvider);
  }

  public ActiveRecordPlugin(String configName, IDataSourceProvider dataSourceProvider) {
    this(configName, dataSourceProvider, DbKit.DEFAULT_TRANSACTION_LEVEL);
  }

  public ActiveRecordPlugin(IDataSourceProvider dataSourceProvider, int transactionLevel) {
    this(DbKit.MAIN_CONFIG_NAME, dataSourceProvider, transactionLevel);
  }

  public ActiveRecordPlugin(Config config) {
    if (config == null) {
      throw new IllegalArgumentException("Config can not be null");
    }
    this.config = config;
  }

  public ActiveRecordPlugin addMapping(String tableName, String primaryKey, Class<? extends Model<?>> modelClass) {
    tableList.add(new Table(tableName, primaryKey, modelClass));
    return this;
  }

  public ActiveRecordPlugin addMapping(String tableName, Class<? extends Model<?>> modelClass) {
    tableList.add(new Table(tableName, modelClass));
    return this;
  }

  public ActiveRecordPlugin addSqlTemplate(String sqlTemplate) {
    config.sqlKit.addSqlTemplate(sqlTemplate);
    return this;
  }

  public ActiveRecordPlugin addSqlTemplate(com.jfinal.template.source.ISource sqlTemplate) {
    config.sqlKit.addSqlTemplate(sqlTemplate);
    return this;
  }

  public ActiveRecordPlugin setBaseSqlTemplatePath(String baseSqlTemplatePath) {
    config.sqlKit.setBaseSqlTemplatePath(baseSqlTemplatePath);
    return this;
  }

  public SqlKit getSqlKit() {
    return config.sqlKit;
  }

  public com.jfinal.template.Engine getEngine() {
    return getSqlKit().getEngine();
  }

  /**
   * Set transaction level define in java.sql.Connection
   * 
   * @param transactionLevel only be 0, 1, 2, 4, 8
   */
  public ActiveRecordPlugin setTransactionLevel(int transactionLevel) {
    config.setTransactionLevel(transactionLevel);
    return this;
  }

  public ActiveRecordPlugin setCache(IDbCache cache) {
    if (cache == null) {
      throw new IllegalArgumentException("cache can not be null");
    }
    config.cache = cache;
    return this;
  }

  public ActiveRecordPlugin setShowSql(boolean showSql) {
    config.showSql = showSql;
    return this;
  }

  public ActiveRecordPlugin setSqlStatementStat(ISqlStatementStat stat, boolean writeSync) {
    config.setSqlStatementStat(stat, writeSync);
    return this;
  }

  public ActiveRecordPlugin setDevMode(boolean devMode) {
    this.devMode = devMode;
    config.setDevMode(devMode);
    return this;
  }

  public Boolean getDevMode() {
    return devMode;
  }

  public ActiveRecordPlugin setDialect(Dialect dialect) {
    if (dialect == null) {
      throw new IllegalArgumentException("dialect can not be null");
    }
    config.dialect = dialect;
    if (config.transactionLevel == Connection.TRANSACTION_REPEATABLE_READ && dialect.isOracle()) {
      // Oracle 不支持 Connection.TRANSACTION_REPEATABLE_READ
      config.transactionLevel = Connection.TRANSACTION_READ_COMMITTED;
    }
    return this;
  }

  public ActiveRecordPlugin setContainerFactory(IContainerFactory containerFactory) {
    if (containerFactory == null) {
      throw new IllegalArgumentException("containerFactory can not be null");
    }
    config.containerFactory = containerFactory;
    return this;
  }

  public ActiveRecordPlugin setDbProFactory(IDbProFactory dbProFactory) {
    if (dbProFactory == null) {
      throw new IllegalArgumentException("dbProFactory can not be null");
    }
    config.dbProFactory = dbProFactory;
    return this;
  }

  /**
   * 当使用 create table 语句创建用于开发使用的数据表副本时，假如create table 中使用的
   * 复合主键次序不同，那么MappingKitGeneretor 反射生成的复合主键次序也会不同。
   * 
   * 而程序中类似于 model.deleteById(id1, id2) 方法中复合主键次序与需要与映射时的次序 保持一致，可以在MappingKit
   * 映射完成以后通过调用此方法再次强制指定复合主键次序
   * 
   * <pre>
   * Example:
   * ActiveRecrodPlugin arp = new ActiveRecordPlugin(...);
   * _MappingKit.mapping(arp);
   * arp.setPrimaryKey("account_role", "account_id, role_id");
   * me.add(arp);
   * </pre>
   */
  public void setPrimaryKey(String tableName, String primaryKey) {
    for (Table table : tableList) {
      if (table.getName().equalsIgnoreCase(tableName.trim())) {
        table.setPrimaryKey(primaryKey);
      }
    }
  }

  public boolean start() {
    if (isStarted) {
      return true;
    }
    if (config.dataSource == null && dataSourceProvider != null) {
      config.dataSource = dataSourceProvider.getDataSource();
    }
    if (config.dataSource == null) {
      throw new RuntimeException("ActiveRecord start error: ActiveRecordPlugin need DataSource or DataSourceProvider");
    }

    config.sqlKit.parseSqlTemplate();

    tableBuilder.build(tableList, config);
    DbKit.addConfig(config);
    isStarted = true;
    log.info("{} start sucessfully",config.getName());
    return true;
  }

  public boolean stop() {
    DbKit.removeConfig(config.getName());
    isStarted = false;
    return true;
  }

  /**
   * 用于分布式场景，当某个分布式节点只需要用 Model 承载和传输数据，而不需要实际操作数据库时 调用本方法可保障
   * IContainerFactory、Dialect、ICache 的一致性
   * 
   * 本用法更加适用于 Generator 生成的继承自 base model的 Model，更加便于传统第三方工具对 带有 getter、setter 的
   * model 进行各种处理
   * 
   * <pre>
   * 警告：Dialect、IContainerFactory、ICache 三者一定要与集群中其它节点保持一致，
   *     以免程序出现不一致行为
   * </pre>
   */
  public static void useAsDataTransfer(Dialect dialect, IContainerFactory containerFactory, IDbCache cache) {
    if (dialect == null) {
      throw new IllegalArgumentException("dialect can not be null");
    }
    if (containerFactory == null) {
      throw new IllegalArgumentException("containerFactory can not be null");
    }
    if (cache == null) {
      throw new IllegalArgumentException("cache can not be null");
    }
    ActiveRecordPlugin arp = new ActiveRecordPlugin(new NullDataSource());
    arp.setDialect(dialect);
    arp.setContainerFactory(containerFactory);
    arp.setCache(cache);
    arp.start();
    DbKit.brokenConfig = arp.config;
  }

  /**
   * 分布式场景下指定 IContainerFactory，并默认使用 MysqlDialect、EhCache
   * 
   * @see #useAsDataTransfer(Dialect, IContainerFactory, IDbCache)
   */
  public static void useAsDataTransfer(IContainerFactory containerFactory) {
    useAsDataTransfer(new com.litongjava.db.activerecord.dialect.MysqlDialect(), containerFactory, new DefaultEhCache());
  }

  /**
   * 分布式场景下指定 Dialect、IContainerFactory，并默认使用 EhCache
   * 
   * @see #useAsDataTransfer(Dialect, IContainerFactory, IDbCache)
   */
  public static void useAsDataTransfer(Dialect dialect, IContainerFactory containerFactory) {
    useAsDataTransfer(dialect, containerFactory, new DefaultEhCache());
  }

  /**
   * 分布式场景下指定 Dialect、 并默认使用 IContainerFactory.defaultContainerFactory、EhCache
   * 
   * @see #useAsDataTransfer(Dialect, IContainerFactory, IDbCache)
   */
  public static void useAsDataTransfer(Dialect dialect) {
    useAsDataTransfer(dialect, IContainerFactory.defaultContainerFactory, new DefaultEhCache());
  }

  /**
   * 分布式场景下默认使用 MysqlDialect、 IContainerFactory.defaultContainerFactory、EhCache
   * 
   * @see #useAsDataTransfer(Dialect, IContainerFactory, IDbCache)
   */
  public static void useAsDataTransfer() {
    useAsDataTransfer(new MysqlDialect(), IContainerFactory.defaultContainerFactory, new DefaultEhCache());
  }

  public Config getConfig() {
    return config;
  }

  /**
   * 一般用于配置 TableBuilder 内的 JavaType
   * 
   * <pre>
   * 例如：
   *    ActiveRecordPlugin arp = ...;
   *    JavaType jt = arp.getTableBuilder().getJavaType();
   *    
   *    jt.addType(org.postgresql.geometric.PGpoint.class);
   *    jt.addType(org.postgresql.geometric.PGbox.class);
   *    jt.addType(org.postgresql.geometric.PGcircle.class);
   *    jt.addType(org.postgresql.geometric.PGline.class);
   *    jt.addType(org.postgresql.geometric.PGlseg.class);
   *    jt.addType(org.postgresql.geometric.PGpath.class);
   *    jt.addType(org.postgresql.geometric.PGpolygon.class);
   * </pre>
   */
  public TableBuilder getTableBuilder() {
    return tableBuilder;
  }

  /**
   * 可用于切换 TableBuilder 实现类
   */
  public ActiveRecordPlugin setTableBuilder(TableBuilder tableBuilder) {
    this.tableBuilder = tableBuilder;
    return this;
  }
}
