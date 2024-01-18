package com.litongjava.jfinal.plugin.activerecord.bean;

import com.litongjava.jfinal.plugin.activerecord.Record;

public interface RecordConvert {
  public <T> T toJavaBean(Record record,Class<T> beanClass);

  public Record fromJavaBean(Object bean);
}
