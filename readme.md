# java-db

Java 操作数据库的瑞士军刀

`java-db` 是一个功能强大的 Java 数据库操作库，支持多种数据库类型，提供简洁高效的数据管理工具。

## 功能特性

- **关系型数据库支持**：SQLite3、H2、AnsiSQL、Informix、MySQL、Oracle、PostgreSQL、SQL Server。
- **缓存数据库支持**：Redis。
- **非关系型数据库支持**：MongoDB。
- **本地缓存支持**：Ehcache。
- **时序数据库支持**：TDengine。

---

## 开源地址

- [GitHub 仓库 1](https://github.com/litongjava/java-db)
- [GitHub 仓库 2](https://github.com/ppnt/java-db)

---

## 快速开始（以 MySQL 为例）

### 1. 创建示例数据库表

运行以下 SQL 脚本来创建一个 `USER` 表并插入一些测试数据：

```sql
CREATE TABLE USER (
  id BIGINT(20) NOT NULL COMMENT '主键ID',
  NAME VARCHAR(30) NULL DEFAULT NULL COMMENT '姓名',
  age INT(11) NULL DEFAULT NULL COMMENT '年龄',
  email VARCHAR(50) NULL DEFAULT NULL COMMENT '邮箱',
  addr VARCHAR(250) NULL DEFAULT NULL COMMENT '地址',
  remark VARCHAR(250) NULL DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (id)
);

INSERT INTO USER (id, NAME, age, email, addr, remark)
VALUES (1, '张三', 25, 'zhangsan@example.com', '北京市朝阳区', '无');
INSERT INTO USER (id, NAME, age, email, addr, remark)
VALUES (2, '李四', 30, 'lisi@example.com', '上海市浦东新区', '无');
INSERT INTO USER (id, NAME, age, email, addr, remark)
VALUES (3, '王五', 35, 'wangwu@example.com', '广州市天河区', '无');
```

---

### 2. 添加 Maven 依赖

在项目的 `pom.xml` 中添加以下依赖：

```xml
<dependency>
  <groupId>com.litongjava</groupId>
  <artifactId>java-db</artifactId>
  <version>{java.db.version}</version>
</dependency>
<dependency>
  <groupId>com.alibaba</groupId>
  <artifactId>druid</artifactId>
  <version>1.1.10</version>
</dependency>
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>5.1.46</version>
</dependency>
```

---

### 3. 编写测试程序

以下是一个示例程序，用于连接 MySQL 数据库并查询数据：

```java
package com.litongjava.tio.web.hello.example;

import java.util.List;

import com.litongjava.db.activerecord.ActiveRecordPlugin;
import com.litongjava.db.activerecord.Db;
import com.litongjava.db.activerecord.OrderedFieldContainerFactory;
import com.litongjava.db.activerecord.Record;
import com.litongjava.db.druid.DruidPlugin;

public class MysqlTestMain {
  public static void main(String[] args) {
    // 数据库配置信息
    String jdbcUrl = "jdbc:mysql://192.168.3.9:3306/mybatis_plus_study";
    String jdbcUser = "root";
    String jdbcPswd = "robot_123456#";

    // 初始化 DruidPlugin 数据库连接池插件
    DruidPlugin druidPlugin = new DruidPlugin(jdbcUrl, jdbcUser, jdbcPswd);

    // 初始化 ActiveRecordPlugin 数据库操作插件
    ActiveRecordPlugin arp = new ActiveRecordPlugin(druidPlugin);
    arp.setContainerFactory(new OrderedFieldContainerFactory());

    // 启动插件
    druidPlugin.start();
    arp.start();

    // 查询 "USER" 表中的数据并打印记录数
    List<Record> records = Db.findAll("USER");
    System.out.println("记录数量: " + records.size());
  }
}
```

## 使用文档

以下是 `java-db` 各功能模块的详细使用文档链接：

- **概述**：了解 `java-db` 的功能与特点  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/01.html)

- **操作数据库入门示例**：简单易懂的数据库操作基础示例  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/02.html)

- **SQL 模板**：使用 SQL 模板快速操作数据库  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/03.html)

- **数据库配置与使用**：详解如何配置并使用数据库  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/04.html)

- **ActiveRecord**：通过 ActiveRecord 简化数据库操作  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/07_ActiveRecordPlugin.html)

- **Model**：灵活的数据模型操作方式  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/08_Model.html)

- **生成器与 Model**：自动化生成 JavaBean 数据模型  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/09_%E7%94%9F%E6%88%90%E5%99%A8%E4%B8%8E%20JavaBean.html)

- **Db Record**：使用 `Db` 和 `Record` 模式操作数据库  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/10_%E7%8B%AC%E5%88%9DDb%20Record%E6%A8%A1%E5%BC%8F.html)

- **分页处理（paginate）**：示例展示如何进行数据分页操作  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/11_paginate%20%E5%88%86%E9%A1%B5.html)

- **数据库事务处理**：灵活的事务管理方式  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/12_%E6%95%B0%E6%8D%AE%E5%BA%93%E4%BA%8B%E5%8A%A1%E5%A4%84%E7%90%86.html)

- **缓存（Cache）支持**：支持多种缓存类型，如 Redis 和 Ehcache  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/13_Cache%20%E7%BC%93%E5%AD%98.html)

- **多数据库方言支持（Dialect）**：适配多种数据库  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/14_Dialect%E5%A4%9A%E6%95%B0%E6%8D%AE%E5%BA%93%E6%94%AF%E6%8C%81.html)

- **表关联操作**：处理表之间的关联关系  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/15_%E8%A1%A8%E5%85%B3%E8%81%94%E6%93%8D%E4%BD%9C.html)

- **复杂主键支持**：应对复杂主键的操作需求  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/16_%E5%A4%8D%E5%90%88%E4%B8%BB%E9%94%AE.html)

- **Oracle 支持**：操作 Oracle 数据库的特性支持  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/17_Oracle%E6%94%AF%E6%8C%81.html)

- **Enjoy SQL 模板**：基于 Enjoy 的轻量级 SQL 模板  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/18_Enjoy%20SQL%20%E6%A8%A1%E6%9D%BF.html)

- **多数据源支持**：灵活处理多个数据源  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/20_%E5%A4%9A%E6%95%B0%E6%8D%AE%E6%BA%90%E6%94%AF%E6%8C%81.html)

- **调用存储过程**：示例演示如何调用存储过程  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/22_%E8%B0%83%E7%94%A8%E5%AD%98%E5%82%A8%E8%BF%87%E7%A8%8B.html)

- **生成 SQL**：动态生成 SQL 的实用方法  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/24.html)

- **通透实体类操作传统数据库**：通过实体类高效操作数据库  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/25.html)

- **读写分离**：实现数据库的读写分离  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/26.html)

- **Spring Boot 整合 Java-DB**：快速集成 Spring Boot 和 `java-db`  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/27.html)

- **SQL 统计**：强大的 SQL 统计功能  
  [查看详情](https://tio-boot.litongjava.com/zh/09_java-db/29.html)

---

## 贡献

欢迎贡献！您可以 Fork 此仓库并提交 Pull Request。

---

## 许可证

本项目遵循 MIT 开源许可证。

---

## 技术支持

有关更多信息和支持，请访问 [GitHub 仓库](https://github.com/litongjava/java-db)。
