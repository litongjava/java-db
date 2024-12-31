package com.litongjava.db.activerecord.dialect;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Row;
import com.litongjava.db.activerecord.Table;
import com.litongjava.tio.utils.json.Json;
import com.litongjava.tio.utils.json.JsonUtils;

/**
 * MysqlDialect.
 */
public class MysqlDialect extends Dialect {

  public String forTableBuilderDoBuild(String tableName) {
    if (tableName.contains(".")) {
      return "select * from " + tableName + " where 1 = 2";
    } else {
      return "select * from `" + tableName + "` where 1 = 2";
    }
  }

  public String forFindAll(String tableName) {
    if (tableName.contains(".")) {
      return "select * from " + tableName;
    } else {
      return "select * from `" + tableName + "`";
    }
  }

  public void forModelSave(Table table, Map<String, Object> attrs, StringBuilder sql, List<Object> paras) {
    sql.append("insert into `").append(table.getName()).append("`(");
    StringBuilder temp = new StringBuilder(") values(");
    for (Entry<String, Object> e : attrs.entrySet()) {
      String colName = e.getKey();
      if (table.hasColumnLabel(colName)) {
        if (paras.size() > 0) {
          sql.append(", ");
          temp.append(", ");
        }
        sql.append('`').append(colName).append('`');
        temp.append('?');
        paras.add(e.getValue());
      }
    }
    sql.append(temp.toString()).append(')');
  }

  public String forModelDeleteById(Table table) {
    String[] pKeys = table.getPrimaryKey();
    StringBuilder sql = new StringBuilder(45);
    String tableName = table.getName();
    if (tableName.contains(".")) {
      sql.append("delete from ").append(tableName);
    } else {
      sql.append("delete from `").append(tableName).append("`");
    }

    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('`').append(pKeys[i]).append("` = ?");
    }
    return sql.toString();
  }

  public void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, StringBuilder sql, List<Object> paras) {
    String tableName = table.getName();
    if (tableName.contains(".")) {
      sql.append("update ").append(tableName);
    } else {
      sql.append("update `").append(tableName).append("`");
    }
    sql.append(" set ");
    String[] pKeys = table.getPrimaryKey();
    for (Entry<String, Object> e : attrs.entrySet()) {
      String colName = e.getKey();
      if (modifyFlag.contains(colName) && !isPrimaryKey(colName, pKeys) && table.hasColumnLabel(colName)) {
        if (paras.size() > 0) {
          sql.append(", ");
        }
        sql.append('`').append(colName).append("` = ? ");
        paras.add(e.getValue());
      }
    }
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('`').append(pKeys[i]).append("` = ?");
      paras.add(attrs.get(pKeys[i]));
    }
  }

  public String forModelFindById(Table table, String columns) {
    String tableName = table.getName();
    String[] pKeys = table.getPrimaryKey();
    return DialectUtils.forDbFindColumnsById(tableName, columns, pKeys);
  }

  public String forDbFindById(String tableName, String[] pKeys) {
    tableName = tableName.trim();
    DialectUtils.trimPrimaryKeys(pKeys);

    StringBuilder sql = new StringBuilder("select * from ");
    if (tableName.contains(".")) {
      sql.append(tableName);
    } else {
      sql.append("`").append(tableName).append("`");
    }
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('`').append(pKeys[i]).append("` = ?");
    }
    return sql.toString();
  }

  public String forDbDeleteById(String tableName, String[] pKeys) {
    tableName = tableName.trim();
    DialectUtils.trimPrimaryKeys(pKeys);
    StringBuilder sql = new StringBuilder("delete from ");
    if (tableName.contains(".")) {
      sql.append(tableName);
    } else {
      sql.append("`").append(tableName).append("`");
    }

    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('`').append(pKeys[i]).append("` = ?");
    }
    return sql.toString();
  }

  /**
   * Do not delete the String[] pKeys parameter, the element of pKeys needs to trim()
   */
  public void forDbSave(String tableName, String[] pKeys, Row record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    DialectUtils.trimPrimaryKeys(pKeys); // important

    if (tableName.contains(".")) {
      sql.append("insert into ").append(tableName).append("(");
    } else {
      sql.append("insert into `").append(tableName).append("`(");
    }

    StringBuilder temp = new StringBuilder();
    temp.append(") values(");

    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      if (paras.size() > 0) {
        sql.append(", ");
        temp.append(", ");
      }
      sql.append('`').append(e.getKey()).append('`');
      temp.append('?');
      paras.add(e.getValue());
    }
    sql.append(temp.toString()).append(')');
  }

  @Override
  public StringBuffer forDbFind(String tableName, String columns, Row record, List<Object> paras) {
    StringBuffer sql = new StringBuffer();
    tableName = tableName.trim();
    sql.append("select ").append(columns).append(" from ");
    if (tableName.contains(".")) {
      sql.append(tableName);
    } else {
      sql.append("`").append(tableName).append("`");
    }

    if (!record.getColumns().isEmpty()) {
      sql.append(" where ");
      boolean first = true;

      for (Entry<String, Object> e : record.getColumns().entrySet()) {
        if (!first) {
          sql.append(" and ");
        } else {
          first = false;
        }
        sql.append('`').append(e.getKey()).append("` = ?");
        paras.add(e.getValue());
      }
    }
    return sql;
  }

  @Override
  public StringBuffer forDbFindByField(String tableName, String columns, String field, Object fieldValue, List<Object> paras) {
    StringBuffer sql = new StringBuffer();
    tableName = tableName.trim();
    sql.append("select ").append(columns).append(" from ");
    if (tableName.contains(".")) {
      sql.append(tableName);
    } else {
      sql.append("`").append(tableName).append("`");
    }

    if (field != null && !field.isEmpty()) {
      sql.append(" where ");
      sql.append('`').append(field).append("` = ?");
      paras.add(fieldValue);
    }
    return sql;
  }

  @Override
  public void forDbDelete(String tableName, String[] pKeys, Row record, StringBuilder sql, List<Object> paras) {
    DialectUtils.forDbDelete(tableName, pKeys, record, sql, paras);
  }

  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Row record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    DialectUtils.trimPrimaryKeys(pKeys);

    // Record 新增支持 modifyFlag
    Set<String> modifyFlag = CPI.getModifyFlag(record);

    if (tableName.contains(".")) {
      sql.append("update ").append(tableName).append(" set ");
    } else {
      sql.append("update `").append(tableName).append("` set ");
    }

    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      String colName = e.getKey();
      if (modifyFlag.contains(colName) && !isPrimaryKey(colName, pKeys)) {
        if (paras.size() > 0) {
          sql.append(", ");
        }
        sql.append('`').append(colName).append("` = ? ");
        paras.add(e.getValue());
      }
    }
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('`').append(pKeys[i]).append("` = ?");
      paras.add(ids[i]);
    }
  }

  public String forPaginate(int pageNumber, int pageSize, StringBuilder findSql) {
    int offset = pageSize * (pageNumber - 1);
    findSql.append(" limit ").append(offset).append(", ").append(pageSize); // limit can use one or two '?' to pass paras
    return findSql.toString();
  }

  @Override
  public String forDbFindColumnsById(String tableName, String columns, String[] pKeys) {
    return DialectUtils.forDbFindColumnsById(tableName, columns, pKeys);
  }

  @Override
  public String forDbFindColumns(String tableName, String columns) {
    return DialectUtils.forDbFindColumns(tableName, columns);
  }

  @Override
  public String forExistsByFields(String tableName, String fields) {
    return DialectUtils.forExistsByFields(tableName, fields);
  }

  @Override
  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Row record, StringBuilder sql, List<Object> paras, String[] jsonFields) {
    if (jsonFields != null) {
      for (String f : jsonFields) {
        record.set(f, Json.getJson().toJson(record.get(f)));
      }
    }
    forDbUpdate(tableName, pKeys, ids, record, sql, paras);
  }

  @Override
  public void transformJsonFields(Row record, String[] jsonFields) {
    if (jsonFields != null) {
      for (String f : jsonFields) {
        Object object = record.get(f);
        if (object != null) {
          String value = Json.getJson().toJson(object);
          record.set(f, value);
        }
      }
    }
  }

  @Override
  public void transformJsonFields(List<Row> recordList, String[] jsonFields) {
    if (jsonFields != null && jsonFields.length > 0) {
      for (String f : jsonFields) {
        for (Row record : recordList) {
          Object object = record.get(f);
          if (object != null) {
            String value = Json.getJson().toJson(object);
            record.set(f, value);
          }
        }
      }
    }
  }

  public void fillPst(PreparedStatement pst, int i, Object value) throws SQLException {
    if (value instanceof String) {
      pst.setString(i + 1, (String) value);
    } else if (value instanceof java.util.Date) {
      if (value instanceof java.sql.Date) {
        pst.setDate(i + 1, (java.sql.Date) value);
      } else if (value instanceof java.sql.Timestamp) {
        pst.setTimestamp(i + 1, (java.sql.Timestamp) value);
      } else {
        // Oracle, SQL Server TIMESTAMP/DATE support new Date()
        java.util.Date d = (java.util.Date) value;
        pst.setTimestamp(i + 1, new java.sql.Timestamp(d.getTime()));
      }
    } else if (value instanceof Long) {
      pst.setLong(i + 1, (Long) value);
    } else if (value instanceof Integer) {
      pst.setInt(i + 1, (Integer) value);
    } else if (value instanceof Short) {
      pst.setShort(i + 1, (Short) value);
    } else if (value instanceof Byte) {
      pst.setByte(i + 1, (Byte) value);
    } else if (value instanceof Double) {
      pst.setDouble(i + 1, (Double) value);
    } else if (value instanceof Float) {
      pst.setFloat(i + 1, (Float) value);
    } else if (value instanceof BigDecimal) {
      pst.setBigDecimal(i + 1, (BigDecimal) value);
    } else if (value instanceof Boolean) {
      pst.setBoolean(i + 1, (Boolean) value);
    } else if (value instanceof java.time.LocalDate) {
      pst.setDate(i + 1, java.sql.Date.valueOf((java.time.LocalDate) value));
    } else if (value instanceof java.time.LocalDateTime) {
      pst.setTimestamp(i + 1, java.sql.Timestamp.valueOf((java.time.LocalDateTime) value));
    } else if (value instanceof byte[]) {
      pst.setBytes(i + 1, (byte[]) value);
    } else if (value instanceof UUID) {
      pst.setObject(i + 1, value, java.sql.Types.OTHER);
    } else if (value instanceof Enum<?>) {
      pst.setString(i + 1, ((Enum<?>) value).name());
    } else if (value instanceof List<?>) {
      // Assuming list of strings; adjust type as needed
      Array sqlArray = pst.getConnection().createArrayOf("text", ((List<?>) value).toArray());
      pst.setArray(i + 1, sqlArray);
    } else {
      String json = JsonUtils.toJson(value);
      pst.setObject(i + 1, json);
    }
  }

  @Override
  public String forColumns(String columns) {
    return DialectUtils.forColumns(columns);
  }

}
