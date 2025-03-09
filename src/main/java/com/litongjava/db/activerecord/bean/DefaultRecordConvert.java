package com.litongjava.db.activerecord.bean;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import com.litongjava.db.activerecord.Row;
import com.litongjava.db.annotation.ATableField;
import com.litongjava.db.annotation.ATableName;
import com.litongjava.record.RecordConvert;
import com.litongjava.tio.utils.name.CamelNameUtils;

public class DefaultRecordConvert implements RecordConvert {

  @Override
  public <T> T toJavaBean(Row record, Class<T> beanClass) {
    try {
      T bean = beanClass.getDeclaredConstructor().newInstance();
      Field[] fields = beanClass.getDeclaredFields();
      for (Field field : fields) {
        field.setAccessible(true);
        String fieldName = field.getName();
        String columnName;

        // 处理 ATableField 注解
        ATableField tableFieldAnnotation = field.getAnnotation(ATableField.class);
        if (tableFieldAnnotation != null && !tableFieldAnnotation.value().isEmpty()) {
          columnName = tableFieldAnnotation.value();
        } else {
          columnName = CamelNameUtils.toUnderscore(fieldName);
        }

        Object fieldValue = record.get(columnName);
        if (fieldValue != null) {
          // 进行类型转换
          if (tableFieldAnnotation != null && tableFieldAnnotation.targetType() != Object.class) {
            fieldValue = convertType(fieldValue, tableFieldAnnotation.targetType());
          }
          try {
            field.set(bean, fieldValue);
          } catch (java.lang.IllegalArgumentException e) {
            String name = fieldValue.getClass().getName();
            String message = "Failed to set " + columnName + ",the value is " + fieldValue + " and value type is " + name;
            throw new RuntimeException(message, e);
          }

        }
      }
      return bean;
    } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error converting Record to Bean", e);
    }
  }

  @Override
  public Row fromJavaBean(Object bean) {
    Row record = new Row();
    Class<?> beanClass = bean.getClass();

    // 处理 ATableName 注解
    ATableName tableNameAnnotation = beanClass.getAnnotation(ATableName.class);
    if (tableNameAnnotation != null) {
      record.setTableName(tableNameAnnotation.value());
    } else {
      record.setTableName(CamelNameUtils.toUnderscore(beanClass.getSimpleName()));
    }

    Field[] fields = beanClass.getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      String fieldName = field.getName();
      String columnName;

      // 处理 ATableField 注解
      ATableField tableFieldAnnotation = field.getAnnotation(ATableField.class);
      if (tableFieldAnnotation != null && !tableFieldAnnotation.value().isEmpty()) {
        columnName = tableFieldAnnotation.value();
      } else {
        columnName = CamelNameUtils.toUnderscore(fieldName);
      }

      try {
        Object value = field.get(bean);
        if (value != null) {
          record.set(columnName, value);
        }

      } catch (IllegalAccessException e) {
        throw new RuntimeException("Error accessing field: " + fieldName, e);
      }
    }

    return record;
  }

  // 类型转换方法
  private Object convertType(Object value, Class<?> targetType) {
    if (targetType == Short.class) {
      return Short.valueOf(value.toString());
    } else if (targetType == Integer.class) {
      return Integer.valueOf(value.toString());
    }
    // 其他类型转换
    return value;
  }
}
