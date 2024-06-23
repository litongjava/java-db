package com.litongjava.jfinal.plugin.activerecord.dialect;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.litongjava.jfinal.plugin.activerecord.CPI;
import com.litongjava.jfinal.plugin.activerecord.Record;
import com.litongjava.jfinal.plugin.activerecord.Table;
import com.litongjava.tio.utils.json.Json;

/**
 * MysqlDialect.
 */
public class MysqlDialect extends Dialect {

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

  public void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, StringBuilder sql,
      List<Object> paras) {
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

  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Record record, StringBuilder sql,
      List<Object> paras) {
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
  public String forExistsByFields(String tableName, String fields) {
    return DialectUtils.forExistsByFields(tableName, fields);
  }

  @Override
  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Record record, StringBuilder sql,
      List<Object> paras, String[] jsonFields) {
    if (jsonFields != null) {
      for (String f : jsonFields) {
        record.set(f, Json.getJson().toJson(record.get(f)));
      }
    }
    forDbUpdate(tableName, pKeys, ids, record, sql, paras);
  }
  
  @Override
  public void forDbSave(String tableName, String[] pKeys, Record record, StringBuilder sql, List<Object> paras,
      String[] jsonFields) {
    if (jsonFields != null) {
      for (String f : jsonFields) {
        Object object = record.get(f);
        if (object != null) {
          String value = Json.getJson().toJson(object);
          record.set(f, value);
        }

      }
    }
    this.forDbSave(tableName, pKeys, record, sql, paras);
  }

}
