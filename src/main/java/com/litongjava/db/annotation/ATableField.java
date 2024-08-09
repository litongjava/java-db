package com.litongjava.db.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ATableField {

  /**
   * 
   * @return
   */
  String value();

  /**
   * 类型
   * @return
   */
  Class<?> targetType() default Object.class;
}
