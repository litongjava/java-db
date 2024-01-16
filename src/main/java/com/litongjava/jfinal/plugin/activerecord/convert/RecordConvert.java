package com.litongjava.jfinal.plugin.activerecord.convert;

import com.litongjava.jfinal.plugin.activerecord.Record;

public interface RecordConvert {
  <T> T toJavaBean(Record record,Class<T> beanClass);
}
