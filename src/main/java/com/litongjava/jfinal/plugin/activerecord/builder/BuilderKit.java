package com.litongjava.jfinal.plugin.activerecord.builder;

import com.litongjava.jfinal.plugin.activerecord.ModelBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * JDBC 获取 Byte 和 Short 时，把 null 转换成了 0，很多时候 0 是有意义的，容易引发业务错误
 *
 * @author tanyaowu
 */
public class BuilderKit {

  public static Byte getByte(ResultSet rs, int i) throws SQLException {
    Object value = rs.getObject(i);
    if (value != null) {
      value = Byte.parseByte(value + "");
      return (Byte) value;
    } else {
      return null;
    }
  }

  public static Short getShort(ResultSet rs, int i) throws SQLException {
    Object value = rs.getObject(i);
    if (value != null) {
      value = Short.parseShort(value + "");
      return (Short) value;
    } else {
      return null;
    }
  }

  /**
   * @param rs
   * @param types
   * @param i
   * @return
   * @throws SQLException
   */
  public static Object getColumnValue(int[] types, ResultSet rs, int i) throws SQLException {
    Object value;
    if (types[i] < Types.DATE) {
      value = rs.getObject(i);
    } else {
      if (types[i] == Types.TIMESTAMP) {
        value = rs.getTimestamp(i);
      } else if (types[i] == Types.DATE) {
        value = rs.getDate(i);
      } else if (types[i] == Types.CLOB) {
        value = ModelBuilder.me.handleClob(rs.getClob(i));
      } else if (types[i] == Types.NCLOB) {
        value = ModelBuilder.me.handleClob(rs.getNClob(i));
      } else if (types[i] == Types.BLOB) {
        value = ModelBuilder.me.handleBlob(rs.getBlob(i));
      } else if (types[i] == Types.ARRAY) {
				value = ModelBuilder.me.handleArray(rs.getArray(i));
      } else {
        value = rs.getObject(i);
      }
    }
    return value;
  }
}

