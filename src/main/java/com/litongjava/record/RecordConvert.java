package com.litongjava.record;

import com.litongjava.db.activerecord.Row;

public interface RecordConvert {
  public <T> T toJavaBean(Row record,Class<T> beanClass);

  public Row fromJavaBean(Object bean);
}
