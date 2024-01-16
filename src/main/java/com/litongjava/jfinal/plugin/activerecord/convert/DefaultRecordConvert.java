package com.litongjava.jfinal.plugin.activerecord.convert;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import com.litongjava.jfinal.plugin.activerecord.Record;

public class DefaultRecordConvert implements RecordConvert {

  @Override
  public <T> T toJavaBean(Record record, Class<T> beanClass) {
    try {
      T bean = beanClass.getDeclaredConstructor().newInstance();
      Field[] fields = beanClass.getDeclaredFields();
      for (Field field : fields) {
        field.setAccessible(true);
        String fieldName = field.getName();
        Object fieldValue = record.get(fieldName);
        if (fieldValue != null) {
          field.set(bean, fieldValue);
        }
      }
      return bean;
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException("Error converting Record to Bean", e);
    }
  }

}
