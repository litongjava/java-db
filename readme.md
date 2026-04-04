# java‑db

> A lightweight, zero-configuration Java database operation framework supporting multiple data sources, read‑write splitting, SQL template management, ActiveRecord ORM, batch operations, transactions, statistics, and more.

## 🚀 Features

- ✅ Supports MySQL, PostgreSQL, Oracle, SQLite, and more  
- ✅ Built‑in Druid/HikariCP connection pools  
- ✅ ActiveRecord ORM + generic Row mode  
- ✅ Enjoy SQL template management (#namespace/#sql/#para)  
- ✅ Read‑write splitting (automatic master‑slave routing)  
- ✅ Batch Save/Update/Delete  
- ✅ Flexible transactions (Db.tx, declarative Tx)  
- ✅ SQL execution statistics (LiteSqlStatementStat)  
- ✅ Guava Striped locks for concurrency control  
- ✅ Multi‑data source & sharding support  
- ✅ Spring Boot integration / JUnit testing  
- ✅ Native Ehcache & Redis caching integration  

---

## 📦 Quick Start

### Maven Dependencies

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

## ⚙️ Configuration

`app.properties`:

```properties
DATABASE_DSN=postgresql://user:pass@127.0.0.1:5432/dbname
DATABASE_DSN_REPLICAS=postgresql://user:pass@127.0.0.1:5433/dbname
jdbc.showSql=true

redis.host=127.0.0.1
redis.port=6379
redis.cacheName=main
redis.timeout=15000
```

### Java Initialization (Spring Boot example)

```java
@Configuration
public class DbConfig {
  @Bean(destroyMethod="stop")
  public ActiveRecordPlugin arp(DataSource ds) {
    ActiveRecordPlugin arp = new ActiveRecordPlugin(ds);
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

## 🎯 Core API

### CRUD (Row Mode)

```java
Row r = new Row().set("name","Alice").set("age",30);
Db.save("user", r);

List<Row> list = Db.find("select * from user where age>?", 20);
Row one = Db.findFirst("select * from user where id=?", 1);
Db.update("update user set age=? where id=?", 31, 1);
Db.deleteById("user", 1);
```

### ActiveRecord (Model Mode)

```java
public class User extends Model<User> {
  public static final User dao = new User().dao();
}
User.dao.findById(1);
new User().set("name","Bob").save();
```

### SQL Templates (Enjoy)

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

### Batch Operations

```java
List<Row> rows = ...;
Db.batchSave("user", rows, 500);
Db.batchUpdate("user", rows, 500);
```

### Transactions

```java
Db.tx(() -> {
  Db.update("update account set balance=balance-? where id=?",100,1);
  Db.update("update account set balance=balance+? where id=?",100,2);
  return true;
});
```

### Read‑Write Splitting

```java
Db.countTable("student");            // automatically uses replica (read)
Db.use("main").update(...);          // forces write to master
```

### SQL Statistics

```java
Lite.querySqlStatementStats();
```

---

## 💾 Caching

### Ehcache

Loads configuration from `classpath:ehcache.xml` by default.

```java
CacheKit.put("users","key","value");
String v = CacheKit.get("users","key");
CacheKit.remove("users","key");
```

### Redis

#### Basic Usage

```java
// String
Redis.use().setStr("foo","bar");
String foo = Redis.use().getStr("foo");

// Bean
Redis.use().setBean("user:1",3600,new User(1,"Alice"));
User u = Redis.use().getBean("user:1",User.class);

// Native Jedis lambda
Long counter = Redis.call(j -> j.incr("counter"));

// Distributed lock
String lockId = Redis.use().lock("lockName",30,5);
if(lockId!=null){ try{/*...*/} finally{ Redis.use().unlock("lockName",lockId);} }
```

#### Cacheable Annotation

```java
@Before(RedisCacheInterceptor.class)
@Cacheable(name="users",value="findById",ttl=600)
public User findById(Long id){ ... }
```

---

## 🧪 Unit Testing

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

## 📖 Documentation & Links

- GitHub: https://github.com/litongjava/java-db  
- Document : https://www.tio-boot.com/zh/09_java-db/01.html
---

## 📝 License

Apache‑2.0