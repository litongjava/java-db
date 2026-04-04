package nexus.io.kit;

import com.litongjava.tio.utils.name.CamelNameUtils;

import nexus.io.db.annotation.ATableName;

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
