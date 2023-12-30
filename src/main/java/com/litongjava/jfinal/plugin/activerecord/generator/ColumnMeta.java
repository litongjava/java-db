package com.litongjava.jfinal.plugin.activerecord.generator;


import java.io.Serializable;

/**
 * ColumnMeta
 */
@SuppressWarnings("serial")
public class ColumnMeta implements Serializable {

  public String name; // 字段名
  public String javaType; // 字段对应的 java 类型
  public String attrName; // 字段对应的属性名

  // ---------

  /**
  -----------+---------+------+-----+---------+----------------
   Field     | Type    | Null | Key | Default | Remarks
  -----------+---------+------+-----+---------+----------------
   id		   | int(11) | NO	| PRI | NULL	| remarks here
  */
  public String type; // 字段类型(附带字段长度与小数点)，例如：decimal(11,2)
  public String isNullable; // 是否允许空值
  public String isPrimaryKey; // 是否主键
  public String defaultValue; // 默认值
  public String remarks; // 字段备注

  public Boolean isAutoIncrement; // 是否自增
}
