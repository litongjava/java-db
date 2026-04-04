package nexus.io.kit;

import nexus.io.db.annotation.ATableName;
import nexus.io.tio.utils.name.CamelNameUtils;

public class DbTableNameUtils {

  public static String getTableName(Class<?> beanClass) {

    // 处理 ATableName 注解
    ATableName tableNameAnnotation = beanClass.getAnnotation(ATableName.class);
    if (tableNameAnnotation != null) {
      return tableNameAnnotation.value();
    } else {
      return CamelNameUtils.toUnderscore(beanClass.getSimpleName());
    }
  }
}
