# java‑db

> 一个轻量级、零配置、支持多数据源、读写分离、SQL 模板管理、ActiveRecord ORM、批量操作、事务、统计等功能的 Java 数据库操作框架。

## 🚀 特性

- ✅ 支持 MySQL、PostgreSQL、Oracle、SQLite 等多种数据库
- ✅ 内置 Druid/HikariCP 连接池
- ✅ ActiveRecord ORM + 通用 Row 模式
- ✅ Enjoy SQL 模板管理（#namespace/#sql/#para）
- ✅ 读写分离（主从自动路由）
- ✅ 支持批量 Save/Update/Delete
- ✅ 灵活事务（Db.tx、声明式 Tx）
- ✅ SQL 执行统计（LiteSqlStatementStat）
- ✅ Guava Striped 分段锁并发控制
- ✅ 多数据源 & 分片支持
- ✅ 集成 Spring Boot／JUnit 测试
- ✅ 原生集成 Ehcache & Redis 缓存

---

## 📦 快速开始

### Maven 依赖

```xml
<dependency>
  <groupId>nexus.io</groupId>
  <artifactId>java-db</artifactId>
  <version>1.5.9</version>
</dependency>
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>8.0.33</version>
</dependency>
<dependency>
  <groupId>com.zaxxer</groupId>
  <artifactId>HikariCP</artifactId>
  <version>5.0.1</version>
</dependency>
<!-- Ehcache -->
<dependency>
  <groupId>net.sf.ehcache</groupId>
  <artifactId>ehcache-core</artifactId>
  <version>2.6.11</version>
</dependency>
<!-- Jedis for Redis -->
<dependency>
  <groupId>redis.clients</groupId>
  <artifactId>jedis</artifactId>
  <version>4.3.1</version>
</dependency>

```

---

## ⚙️ 配置

`app.properties` ：

```properties
DATABASE_DSN=postgresql://user:pass@127.0.0.1:5432/dbname
DATABASE_DSN_REPLICAS=postgresql://user:pass@127.0.0.1:5433/dbname
jdbc.showSql=true

redis.host=127.0.0.1
redis.port=6379
redis.cacheName=main
redis.timeout=15000
```

### Java 初始化（示例：Spring Boot）

```java
@Configuration
public class DbConfig {
  @Bean(destroyMethod="stop")
  public ActiveRecordPlugin arp() {

     String dsn = EnvUtils.get("DATABASE_DSN");
    if (dsn == null) {
      return null;
    }

    JdbcInfo jdbc = new DbDSNParser().parse(dsn);
    int maximumPoolSize = EnvUtils.getInt("jdbc.MaximumPoolSize", 1);
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(jdbc.getUrl());
    config.setUsername(jdbc.getUser());
    config.setPassword(jdbc.getPswd());
    config.setMaximumPoolSize(maximumPoolSize);
    HikariDataSource hikariDataSource = new HikariDataSource(config);

    // 设置数据源
    DsContainer.setDataSource(hikariDataSource);

    ActiveRecordPlugin arp = new ActiveRecordPlugin(hikariDataSource);
    arp.setDialect(new PostgreSqlDialect());
    arp.setShowSql(true);
    arp.getEngine().setSourceFactory(new ClassPathSourceFactory());
    arp.addSqlTemplate("/sql/all.sql");
    arp.start();
    return arp;
  }

  @Bean(destroyMethod="stop")
  public EhCachePlugin ehCachePlugin() {
    EhCachePlugin plugin = new EhCachePlugin();
    plugin.start();
    return plugin;
  }

  @Bean(destroyMethod="stop")
  public RedisPlugin redisPlugin() {
    RedisPlugin rp = new RedisPlugin("main","127.0.0.1",6379,15000,null,0);
    rp.start();
    return rp;
  }
}
```

---

## 🎯 核心 API

### CRUD（Row 模式）

```java
Row r = new Row().set("name","Alice").set("age",30);
Db.save("user", r);

List<Row> list = Db.find("select * from user where age>?", 20);
Row one = Db.findFirst("select * from user where id=?", 1);
Db.update("update user set age=? where id=?", 31, 1);
Db.deleteById("user", 1);
```

### ActiveRecord（Model 模式）

```java
public class User extends Model<User> {
  public static final User dao = new User().dao();
}
User.dao.findById(1);
new User().set("name","Bob").save();
```

### SQL 模板（Enjoy）

```sql
-- src/main/resources/sql/all.sql
#include("user.sql")
```

```sql
-- src/main/resources/sql/user.sql
#namespace("user")
  #sql("findByName")
    select * from user where name like #para(name)
  #end
#end
```

```java
List<Row> users = Db.template("user.findByName", Kv.by("name","%John%")).find();
```

### 批量

```java
List<Row> rows = ...;
Db.batchSave("user", rows, 500);
Db.batchUpdate("user", rows, 500);
```

### 事务

```java
Db.tx(() -> {
  Db.update("update account set balance=balance-? where id=?",100,1);
  Db.update("update account set balance=balance+? where id=?",100,2);
  return true;
});
```

### 读写分离

```java
Db.countTable("student");            // 自动走读库
Db.use("main").update(...);          // 强制写库
```

### SQL 统计

```java
Lite.querySqlStatementStats();
```

---

## 💾 缓存

### Ehcache

默认从 `classpath:ehcache.xml` 加载配置。

```java
CacheKit.put("users","key","value");
String v = CacheKit.get("users","key");
CacheKit.remove("users","key");
```

### Redis

#### Redis

```java
// String
Redis.use().setStr("foo","bar");
String foo = Redis.use().getStr("foo");

// Bean
Redis.use().setBean("user:1",3600,new User(1,"Alice"));
User u = Redis.use().getBean("user:1",User.class);

// Lambda 原生 Jedis
Long counter = Redis.call(j -> j.incr("counter"));

// 分布式锁
String lockId = Redis.use().lock("lockName",30,5);
if(lockId!=null){ try{/*...*/} finally{ Redis.use().unlock("lockName",lockId);} }
```

#### Cacheable 注解

```java
@Before(RedisCacheInterceptor.class)
@Cacheable(name="users",value="findById",ttl=600)
public User findById(Long id){ ... }
```

---

## 🧪 单元测试

```java
@BeforeClass
public static void init() {
  EnvUtils.load();
  new DbConfig().config();
}
@Test
public void testFind() {
  Row r = Db.findFirst("select 1");
  assertNotNull(r);
}
```

---

## 📖 文档 & 链接

- GitHub：https://github.com/litongjava/java-db
- Document https://www.tio-boot.com/zh/09_java-db/01.html

---

## 📝 许可证

Apache‑2.0
