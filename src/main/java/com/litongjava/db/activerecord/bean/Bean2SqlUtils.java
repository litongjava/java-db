package com.litongjava.db.activerecord.bean;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import com.litongjava.db.annotation.ATableField;
import com.litongjava.db.annotation.ATableName;
import com.litongjava.tio.utils.name.CamelNameUtils;

public class Bean2SqlUtils {

  public static String toCreateTableSql(Class<?> entityClass) {
    StringBuilder sql = new StringBuilder();
    String tableName = getTableName(entityClass);

    sql.append("DROP TABLE IF EXISTS ").append(tableName).append(";\n");

    sql.append("CREATE TABLE ").append(tableName).append(" (\n");

    List<String> columns = new ArrayList<>();
    for (Field field : entityClass.getDeclaredFields()) {
      String columnDefinition = getColumnDefinition(field);
      if (columnDefinition != null) {
        columns.add(columnDefinition);
      }
    }

    // 添加额外的必需字段
    columns.add("remark VARCHAR(256)");
    columns.add("creator VARCHAR(64) DEFAULT ''");
    columns.add("create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP");
    columns.add("updater VARCHAR(64) DEFAULT ''");
    columns.add("update_time TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP");
    columns.add("deleted SMALLINT NOT NULL DEFAULT 0");
    columns.add("tenant_id BIGINT NOT NULL DEFAULT 0");

    sql.append(String.join(",\n", columns));
    sql.append("\n);");

    return sql.toString();
  }

  private static String getTableName(Class<?> beanClass) {
    ATableName tableNameAnnotation = beanClass.getAnnotation(ATableName.class);
    if (tableNameAnnotation != null) {
      return tableNameAnnotation.value();
    } else {
      return CamelNameUtils.toUnderscore(beanClass.getSimpleName());
    }
  }

  /**
   * 对于 UUID 类型的 id，需要在数据库中启用 uuid-ossp 扩展。在 PostgreSQL 中，可以使用以下命令启用：sqlCopyCREATE EXTENSION IF NOT EXISTS "uuid-ossp";
   * @param field
   * @return
   */
  private static String getColumnDefinition(Field field) {
    field.setAccessible(true);
    String fieldName = field.getName();
    String columnName;

    ATableField tableFieldAnnotation = field.getAnnotation(ATableField.class);
    if (tableFieldAnnotation != null && !tableFieldAnnotation.value().isEmpty()) {
      columnName = tableFieldAnnotation.value();
    } else {
      columnName = CamelNameUtils.toUnderscore(fieldName);
    }

    Class<?> fieldType = field.getType();

    if (fieldName.equals("id")) {
      if (fieldType == Integer.class || fieldType == int.class) {
        return "id SERIAL PRIMARY KEY";
      } else if (fieldType == Long.class || fieldType == long.class) {
        return "id BIGINT PRIMARY KEY";
      } else if (fieldType == String.class) {
        return "id UUID PRIMARY KEY DEFAULT uuid_generate_v4()";
      }
    } else if (fieldType == Long.class || fieldType == long.class) {
      return columnName + " BIGINT";
    } else if (fieldType == Integer.class || fieldType == int.class) {
      return columnName + " INTEGER";
    } else if (fieldType == Boolean.class || fieldType == boolean.class) {
      return columnName + " BOOLEAN";
    } else if (fieldType == String.class) {
      return columnName + " VARCHAR";
    } else if (fieldType == OffsetDateTime.class) {
      return columnName + " TIMESTAMP WITHOUT TIME ZONE";
    }

    // 如果是不支持的类型，返回null
    return null;
  }
}