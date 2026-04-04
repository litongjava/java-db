package nexus.io.record;

import nexus.io.db.activerecord.Row;

public interface RecordConvert {
  public <T> T toJavaBean(Row record,Class<T> beanClass);

  public Row fromJavaBean(Object bean);
}
