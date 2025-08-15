package com.litongjava.db.activerecord.bean;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.litongjava.db.DbJsonObject;
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
      List<Field> fieldList = new ArrayList<>();

      Field[] fields = beanClass.getDeclaredFields();
      fieldList.addAll(Arrays.asList(fields));

      Class<?> superClass = beanClass.getSuperclass();

      while (superClass != null) {
        fieldList.addAll(Arrays.asList(superClass.getDeclaredFields()));
        superClass = superClass.getSuperclass();
      }

      for (Field javaField : fieldList) {
        javaField.setAccessible(true);
        String fieldName = javaField.getName();
        String columnName;

        // 处理 ATableField 注解
        ATableField tableFieldAnnotation = javaField.getAnnotation(ATableField.class);
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
          } else if (javaField.getType().equals(Short.class)) {
            fieldValue = convertShortObject(fieldValue);

          } else if (javaField.getType().equals(Long.class)) {
            fieldValue = convertLongObject(fieldValue);

          } else if (javaField.getType().equals(Boolean.class)) {
            fieldValue = convertBooleanObject(fieldValue);

          } else if (javaField.getType().equals(Timestamp.class)) {
            fieldValue = convertTimestampObject(fieldValue);

          } else if (javaField.getType().equals(DbJsonObject.class)) {
            fieldValue = convertDbJsonObject(fieldValue);
          }
          try {
            javaField.set(bean, fieldValue);
          } catch (java.lang.IllegalArgumentException e) {
            String name = fieldValue.getClass().getName();
            String message = "Failed to set " + columnName + ",the value is " + fieldValue + " and value type is "
                + name;
            throw new RuntimeException(message, e);
          }
        }
      }
      return bean;
    } catch (InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Error converting Record to Bean", e);
    }
  }

  private Object convertTimestampObject(Object fieldValue) {
    if (fieldValue != null && fieldValue instanceof java.sql.Date) {
      return new Timestamp(((java.sql.Date) fieldValue).getTime());
    }
    return fieldValue;
  }

  private Object convertLongObject(Object fieldValue) {
    if (fieldValue != null && fieldValue instanceof Integer) {
      return Long.valueOf((Integer) fieldValue);
    }
    return fieldValue;
  }

  private Object convertShortObject(Object fieldValue) {
    if (fieldValue != null && fieldValue instanceof Integer) {
      return ((Integer)fieldValue).shortValue();
    }
    return fieldValue;
  }

  private Boolean convertBooleanObject(Object fieldValue) {
    if (fieldValue != null && fieldValue instanceof Number) {
      if (fieldValue.equals(0)) {
        return Boolean.FALSE;
      } else {
        return Boolean.TRUE;
      }
    }
    return Boolean.FALSE;
  }

  private Object convertDbJsonObject(Object fieldValue) {
    return new DbJsonObject(fieldValue.toString());
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
