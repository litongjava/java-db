package com.litongjava.db.activerecord.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Record;
import com.litongjava.db.activerecord.Table;
import com.litongjava.tio.utils.json.Json;

/**
 * MysqlDialect.
 */
public class TdEngineDialect extends Dialect {

  public String forTableBuilderDoBuild(String tableName) {
    return "select * from `" + tableName + "` where 1 = 2";
  }

  public String forFindAll(String tableName) {
    return "select * from `" + tableName + "`";
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
    sql.append("delete from `");
    sql.append(table.getName());
    sql.append("` where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('`').append(pKeys[i]).append("` = ?");
    }
    return sql.toString();
  }

  public void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, StringBuilder sql, List<Object> paras) {
    sql.append("update `").append(table.getName()).append("` set ");
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

    StringBuilder sql = new StringBuilder("select * from `").append(tableName).append("` where ");
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

    StringBuilder sql = new StringBuilder("delete from `").append(tableName).append("` where ");
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
  public void forDbSave(String tableName, String[] pKeys, Record record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    DialectUtils.trimPrimaryKeys(pKeys); // important

    sql.append("insert into `");
    sql.append(tableName).append("`(");
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
  public void forDbDelete(String tableName, String[] pKeys, Record record, StringBuilder sql, List<Object> paras) {
    DialectUtils.forDbDelete(tableName, pKeys, record, sql, paras);
  }

  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Record record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    DialectUtils.trimPrimaryKeys(pKeys);

    // Record 新增支持 modifyFlag
    Set<String> modifyFlag = CPI.getModifyFlag(record);

    sql.append("update `").append(tableName).append("` set ");
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
  public void fillStatement(PreparedStatement pst, Object... paras) throws SQLException {
    for (int i = 0; i < paras.length; i++) {
      Object object = paras[i];
      if (object instanceof String) {
        pst.setString(i + 1, (String) object);
      } else if (object instanceof java.sql.Timestamp) {
        long time = ((java.sql.Timestamp) object).getTime();
        pst.setLong(i + 1, time);
        // parse row block info error:unsupported data type 9
        // pst.setTimestamp(i + 1, ((java.sql.Timestamp) object));
      } else if (object instanceof java.util.Date) {
        long time = ((java.util.Date) object).getTime();
        pst.setLong(i + 1, time);
      } else if (object instanceof java.lang.Integer) {
        pst.setInt(i + 1, (java.lang.Integer) object);
      } else if (object instanceof java.lang.Long) {
        pst.setLong(i + 1, (java.lang.Long) object);

      } else if (object instanceof java.lang.Float) {
        pst.setFloat(i + 1, (java.lang.Float) object);

      } else if (object instanceof java.lang.Double) {
        pst.setDouble(i + 1, (java.lang.Double) object);

      } else if (object instanceof java.lang.Short) {
        pst.setShort(i + 1, (java.lang.Short) object);

      } else if (object instanceof java.lang.Byte) {
        pst.setByte(i + 1, (java.lang.Byte) object);

      } else if (object instanceof byte[]) {
        pst.setBytes(i + 1, (byte[]) object);

      } else {
        pst.setString(i + 1, (String) object);
      }
    }
  }

  @Override
  public void fillStatement(PreparedStatement pst, List<Object> paras) throws SQLException {
    for (int i = 0, size = paras.size(); i < size; i++) {
      pst.setString(i + 1, (String) paras.get(i));
    }
  }

  @Override
  public String forExistsByFields(String tableName, String fields) {
    return DialectUtils.forExistsByFields(tableName, fields);
  }

  @Override
  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Record record, StringBuilder sql, List<Object> paras, String[] jsonFields) {
    if (jsonFields != null) {
      for (String f : jsonFields) {
        record.set(f, Json.getJson().toJson(record.get(f)));
      }
    }
    forDbUpdate(tableName, pKeys, ids, record, sql, paras);

  }

  @Override
  public void transformJsonFields(Record record, String[] jsonFields) {

    if (jsonFields != null && jsonFields.length > 0) {
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
  public void transformJsonFields(List<Record> recordList, String[] jsonFields) {
    if (jsonFields != null && jsonFields.length > 0) {
      for (String f : jsonFields) {
        for (Record record : recordList) {
          Object object = record.get(f);
          if (object != null) {
            String value = Json.getJson().toJson(object);
            record.set(f, value);
          }
        }
      }
    }
  }

  @Override
  public StringBuffer forDbFind(String tableName, String columns, Record record, List<Object> paras) {
    StringBuffer sql = new StringBuffer();
    tableName = tableName.trim();
    sql.append("select ").append(columns).append(" from `").append(tableName).append("`");

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
  public String forColumns(String columns) {
    return DialectUtils.forColumns(columns);
  }
}
