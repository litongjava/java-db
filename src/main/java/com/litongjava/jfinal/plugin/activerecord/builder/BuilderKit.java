package com.litongjava.jfinal.plugin.activerecord.builder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PGobject;

import com.litongjava.jfinal.plugin.activerecord.ModelBuilder;
import com.litongjava.tio.utils.json.Json;

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
    Object value = null;
    if (types[i] < Types.DATE) {
      value = rs.getObject(i);
    } else {
      if (types[i] == Types.TIMESTAMP) {
        value = rs.getTimestamp(i);
      } else if (types[i] == Types.DATE) {
        value = rs.getDate(i);
      } else if (types[i] == Types.OTHER) {
        value = rs.getObject(i);
        if (value instanceof PGobject) {
          PGobject pGobject = (PGobject) value;
          if ("json".equals(pGobject.getType())) {
            String stringValue = pGobject.getValue();
            value = parseJsonField(stringValue);
          } else {
            value = pGobject.getValue();
          }

        }
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

  public static Object parseJsonField(String stringValue) {
    if (stringValue.startsWith("[") && stringValue.endsWith("]")) {
      List<Map<String, Object>> lists = Json.getJson().parseToListMap(stringValue, String.class, Object.class);
      return lists;
    } else if (stringValue.startsWith("{") && stringValue.endsWith("}")) {
      Map<String, Object> map = Json.getJson().parseToMap(stringValue, String.class, Object.class);
      return map;
    } else if (stringValue.startsWith("\"") && stringValue.endsWith("\"")) {
      return Json.getJson().parse(stringValue);
    } else {
      return stringValue;
    }
  }
}
