package com.litongjava.template;

import com.jfinal.template.expr.ast.FieldGetter;
import com.litongjava.db.activerecord.Row;

public class RowFieldGetter extends FieldGetter {

  // 所有 Row 可以共享 RecordFieldGetter 获取属性
  static final RowFieldGetter singleton = new RowFieldGetter();

  public FieldGetter takeOver(Class<?> targetClass, String fieldName) {
    if (Row.class.isAssignableFrom(targetClass)) {
      return singleton;
    } else {
      return null;
    }
  }

  public Object get(Object target, String fieldName) throws Exception {
    return ((Row) target).get(fieldName);
  }
}