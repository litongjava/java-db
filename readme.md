#jfinal-plugins
## JFinal Plugins简介
JFinal自带了一些实用的插件，这些插件默认是整合在JFinal包中的。我从中提取了一些插件，并将它们组织成了一个独立的项目，名为jfinal-plugins。你可以利用jfinal-plugins来连接ehcache，redis以及数据库。
开源地址
https://github.com/litongjava/jfinal-plugins

##使用jfinal-plugins连接数库
1. 使用jfinal-plugins连接数据库
首先，我们创建一个表：

```
CREATE TABLE USER(
  id BIGINT(20) NOT NULL COMMENT '主键ID',
  NAME VARCHAR(30) NULL DEFAULT NULL COMMENT '姓名',
  age INT(11) NULL DEFAULT NULL COMMENT '年龄',
  email VARCHAR(50) NULL DEFAULT NULL COMMENT '邮箱',
  addr VARCHAR(250) NULL DEFAULT NULL COMMENT '地址',
  remark VARCHAR(250) NULL DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (id)
)
```

然后，我们随意插入几条记录：

```
INSERT INTO USER (id, NAME, age, email, addr, remark) VALUES (1, '张三', 25, 'zhangsan@example.com', '北京市朝阳区', '无');
INSERT INTO USER (id, NAME, age, email, addr, remark) VALUES (2, '李四', 30, 'lisi@example.com', '上海市浦东新区', '无');
INSERT INTO USER (id, NAME, age, email, addr, remark) VALUES (3, '王五', 35, 'wangwu@example.com', '广州市天河区', '无');
```

接下来，我们添加依赖项：

```
<dependency>
  <groupId>com.litongjava</groupId>
  <artifactId>jfinal-plugins</artifactId>
  <version>1.0.1</version>
</dependency>
<!-- 连接池 -->
<dependency>
  <groupId>com.alibaba</groupId>
  <artifactId>druid</artifactId>
  <version>1.1.10</version>
</dependency>
<!-- 数据库驱动 -->
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>5.1.46</version>
</dependency>
```

最后，我们编写测试代码：

```
package com.litongjava.jfinal.plugins.mysql;

import com.litongjava.jfinal.plugin.activerecord.ActiveRecordPlugin;
import com.litongjava.jfinal.plugin.activerecord.Db;
import com.litongjava.jfinal.plugin.activerecord.OrderedFieldContainerFactory;
import com.litongjava.jfinal.plugin.activerecord.Record;
import com.litongjava.jfinal.plugin.druid.DruidPlugin;

import java.util.List;

public class MysqlTestMain {
  public static void main(String[] args) {
    String jdbcUrl = "jdbc:mysql://192.168.3.9:3306/mybatis_plus_study";
    String jdbcUser = "root";
    String jdbcPswd = "robot_123456#";

    DruidPlugin druidPlugin = new DruidPlugin(jdbcUrl, jdbcUser, jdbcPswd);

    ActiveRecordPlugin arp = new ActiveRecordPlugin(druidPlugin);
    arp.setContainerFactory(new OrderedFieldContainerFactory());

    druidPlugin.start();
    arp.start();

    List<Record> records = Db.findAll("USER");
    System.out.println(records.size());

  }
}
```

这段代码使用ActiveRecord插件来连接MySQL数据库并执行查询。以下是每部分的详细解释：

1. **导入所需的库**：首先，我们导入了所需的库，包括ActiveRecordPlugin（用于操作数据库的插件），Db（用于数据库操作的类），OrderedFieldContainerFactory（用于设置字段的顺序），Record（用于表示数据库记录的类），以及DruidPlugin（用于数据库连接的插件）。

2. **定义主函数**：在`main`函数中，我们首先定义了数据库的URL、用户名和密码。

3. **创建DruidPlugin对象**：使用数据库的URL、用户名和密码，我们创建了一个DruidPlugin对象，用于管理数据库连接。

4. **创建ActiveRecordPlugin对象**：然后，我们使用刚刚创建的DruidPlugin对象创建了一个ActiveRecordPlugin对象，用于操作数据库。

5. **设置字段的顺序**：通过调用`setContainerFactory`方法并传入一个新的`OrderedFieldContainerFactory`对象，我们设置了数据库字段的顺序。

6. **启动插件**：接着，我们调用`start`方法启动了DruidPlugin和ActiveRecordPlugin插件。

7. **执行查询并打印结果**：最后，我们调用`Db.findAll`方法查询了"USER"表中的所有记录，并打印出了记录的数量。

## 使用jfinal-plugins连接ehcache

首先，我们添加了必要的依赖项：

```
<dependency>
  <groupId>com.litongjava</groupId>
  <artifactId>jfinal-plugins</artifactId>
  <version>1.0.1</version>
</dependency>
<dependency>
  <groupId>net.sf.ehcache</groupId>
  <artifactId>ehcache-core</artifactId>
  <version>2.6.11</version>
</dependency>
```

然后，我们创建了一个名为`UserService`的类，该类有一个名为`getUser`的方法，该方法在被调用时会打印出输入的用户名，并返回这个用户名：

```java
package com.litongjava.jfinal.plugins.ecache;

import com.litongjava.jfinal.aop.Before;
import com.litongjava.jfinal.plugin.cache.CacheName;
import com.litongjava.jfinal.plugin.ehcache.EcacheCacheInterceptor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserService {

  @Before(EcacheCacheInterceptor.class)
  public String getUser(String username) {
    System.out.println("username:" + username);
    return username;
  }
}
```

最后，我们创建了一个名为`EcacheDemo`的类，该类在其`main`方法中启动了`EhCachePlugin`，然后获取了`UserService`的一个实例，并调用了其`getUser`方法三次，最后停止了`EhCachePlugin`：

```java
package com.litongjava.jfinal.plugins.ecache;
import com.litongjava.jfinal.aop.Aop;
import com.litongjava.jfinal.plugin.ehcache.EhCachePlugin;

public class EcacheDemo {
  public static void main(String[] args) {
    EhCachePlugin ehCachePlugin = new EhCachePlugin();
    ehCachePlugin.start();
    UserService userService = Aop.get(UserService.class);
    userService.getUser("litong");
    userService.getUser("litong001");
    userService.getUser("litong");
    ehCachePlugin.stop();
  }
}
```

## 使用jfinal-plugins连接Redis

添加依赖
```
    <dependency>
      <groupId>com.litongjava</groupId>
      <artifactId>jfinal-aop</artifactId>
      <version>1.1.7</version>
    </dependency>
    <dependency>
      <groupId>redis.clients</groupId>
      <artifactId>jedis</artifactId>
      <version>3.6.3</version>
    </dependency>
    <dependency>
      <groupId>de.ruedigermoeller</groupId>
      <artifactId>fst</artifactId>
      <version>2.57</version> <!-- 注意：更高版本不支持 jdk 8 -->
    </dependency>
```

编写代码
创建了一个名为`UserService`的类，该类有一个名为`getUser`的方法并添加了RedisCacheInterceptor注解，该方法在被调用时会打印出输入的用户名，并返回这个用户名：

```java
package com.litongjava.jfinal.plugins.redis;

import com.litongjava.jfinal.aop.Before;
import com.litongjava.jfinal.plugin.redis.RedisCacheInterceptor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UserService {

  @Before(RedisCacheInterceptor.class)
  public String getUser(String username) {
    System.out.println("select from db username:" + username);
    return username;
  }
}
```

然后，我们创建了一个名为`RedisUserDemo`的类，该类在其`main`方法中启动了`RedisPlugin`，然后获取了`UserService`的一个实例，并调用了其`getUser`方法三次，最后停止了`RedisPlugin`：在第一次调用时就会将返回存入到redis中

```java
package com.litongjava.jfinal.plugins.redis;

import com.litongjava.jfinal.aop.Aop;
import com.litongjava.jfinal.plugin.redis.RedisPlugin;

public class RedisUserDemo {

  public static void main(String[] args) {
    // 用于缓存bbs模块的redis服务
    RedisPlugin bbsRedis = new RedisPlugin("bbs", "localhost");
    bbsRedis.start();

    UserService userService = Aop.get(UserService.class);
    userService.getUser("litongjava");
    userService.getUser("litongjava");
    String username = userService.getUser("litongjava");
    System.out.println("result:"+username);
    bbsRedis.stop();
  }
}
```

## Cacheable注解
`RedisCacheInterceptor`和`EcacheCacheInterceptor`可以单独使用，也可以和`@Cacheable`注解配合使用。全类名为`com.litongjava.jfinal.plugin.cache.Cacheable`。下面是一个示例：

```java
@Before(RedisCacheInterceptor.class)
@Cacheable(name = "userService", value = "getUser", ttl = 300)
public String getUser(String username) {
  System.out.println("select from db username:" + username);
  return username;
}
```

在单独使用时及不使用@Cacheable注解时，`@Cacheable`注解的默认`name`值是类名，`value`值是方法名加上所有参数名的HashCode值，`ttl`（Time To Live）值默认为3600秒。具体的实现细节可以参考源码`com.litongjava.jfinal.plugin.cache.CacheableModel.buildCacheModel`。

简单来说，`@Cacheable`注解用于标记一个方法的结果是可以被缓存的。当一个被`@Cacheable`注解的方法被调用时，系统首先会检查缓存中是否已经有了这个方法的结果，如果有，就直接返回缓存的结果，否则，就运行方法并把结果存入缓存中。这样，当我们需要再次调用这个方法时，就可以直接从缓存中获取结果.