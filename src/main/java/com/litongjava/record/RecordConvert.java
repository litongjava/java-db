package com.litongjava.record;

import com.litongjava.db.activerecord.Record;

public interface RecordConvert {
  public <T> T toJavaBean(Record record,Class<T> beanClass);

  public Record fromJavaBean(Object bean);
}
