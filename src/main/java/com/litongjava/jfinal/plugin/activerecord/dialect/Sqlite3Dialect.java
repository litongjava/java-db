package com.litongjava.jfinal.plugin.activerecord.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.litongjava.jfinal.plugin.activerecord.CPI;
import com.litongjava.jfinal.plugin.activerecord.Record;
import com.litongjava.jfinal.plugin.activerecord.Table;
import com.litongjava.jfinal.plugin.activerecord.builder.TimestampProcessedModelBuilder;
import com.litongjava.jfinal.plugin.activerecord.builder.TimestampProcessedRecordBuilder;

/**
 * SqliteDialect.
 */
public class Sqlite3Dialect extends Dialect {

  public Sqlite3Dialect() {
    this.modelBuilder = TimestampProcessedModelBuilder.me;
    this.recordBuilder = TimestampProcessedRecordBuilder.me;
  }

  public String forTableBuilderDoBuild(String tableName) {
    return "select * from " + tableName + " where 1 = 2";
  }

  public void forModelSave(Table table, Map<String, Object> attrs, StringBuilder sql, List<Object> paras) {
    sql.append("insert into ").append(table.getName()).append('(');
    StringBuilder temp = new StringBuilder(") values(");
    for (Entry<String, Object> e : attrs.entrySet()) {
      String colName = e.getKey();
      if (table.hasColumnLabel(colName)) {
        if (paras.size() > 0) {
          sql.append(", ");
          temp.append(", ");
        }
        sql.append(colName);
        temp.append('?');
        paras.add(e.getValue());
      }
    }
    sql.append(temp.toString()).append(')');
  }

  public String forModelDeleteById(Table table) {
    String[] pKeys = table.getPrimaryKey();
    StringBuilder sql = new StringBuilder(45);
    sql.append("delete from ");
    sql.append(table.getName());
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append(pKeys[i]).append(" = ?");
    }
    return sql.toString();
  }

  public void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, StringBuilder sql,
      List<Object> paras) {
    sql.append("update ").append(table.getName()).append(" set ");
    String[] pKeys = table.getPrimaryKey();
    for (Entry<String, Object> e : attrs.entrySet()) {
      String colName = e.getKey();
      if (modifyFlag.contains(colName) && !isPrimaryKey(colName, pKeys) && table.hasColumnLabel(colName)) {
        if (paras.size() > 0) {
          sql.append(", ");
        }
        sql.append(colName).append(" = ? ");
        paras.add(e.getValue());
      }
    }
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append(pKeys[i]).append(" = ?");
      paras.add(attrs.get(pKeys[i]));
    }
  }

  public String forModelFindById(Table table, String columns) {
    StringBuilder sql = new StringBuilder("select ").append(columns).append(" from ");
    sql.append(table.getName());
    sql.append(" where ");
    String[] pKeys = table.getPrimaryKey();
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append(pKeys[i]).append(" = ?");
    }
    return sql.toString();
  }

  public String forDbFindById(String tableName, String[] pKeys) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    StringBuilder sql = new StringBuilder("select * from ").append(tableName).append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append(pKeys[i]).append(" = ?");
    }
    return sql.toString();
  }

  public String forDbDeleteById(String tableName, String[] pKeys) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append(pKeys[i]).append(" = ?");
    }
    return sql.toString();
  }

  public void forDbSave(String tableName, String[] pKeys, Record record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    sql.append("insert into ");
    sql.append(tableName).append('(');
    StringBuilder temp = new StringBuilder();
    temp.append(") values(");

    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      if (paras.size() > 0) {
        sql.append(", ");
        temp.append(", ");
      }
      sql.append(e.getKey());
      temp.append('?');
      paras.add(e.getValue());
    }
    sql.append(temp.toString()).append(')');
  }

  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Record record, StringBuilder sql,
      List<Object> paras) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    // Record 新增支持 modifyFlag
    Set<String> modifyFlag = CPI.getModifyFlag(record);

    sql.append("update ").append(tableName).append(" set ");
    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      String colName = e.getKey();
      if (modifyFlag.contains(colName) && !isPrimaryKey(colName, pKeys)) {
        if (paras.size() > 0) {
          sql.append(", ");
        }
        sql.append(colName).append(" = ? ");
        paras.add(e.getValue());
      }
    }
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append(pKeys[i]).append(" = ?");
      paras.add(ids[i]);
    }
  }

  public String forPaginate(int pageNumber, int pageSize, StringBuilder findSql) {
    int offset = pageSize * (pageNumber - 1);
    findSql.append(" limit ").append(offset).append(", ").append(pageSize);
    return findSql.toString();
  }

  public void fillStatement(PreparedStatement pst, List<Object> paras) throws SQLException {
    fillStatementHandleDateType(pst, paras);
  }

  public void fillStatement(PreparedStatement pst, Object... paras) throws SQLException {
    fillStatementHandleDateType(pst, paras);
  }

  @Override
  public String forDbFindColumnsById(String tableName, String columns, String[] pKeys) {
    return DialectUtils.forDbFindColumnsById(tableName, columns, pKeys);
  }
  
  @Override
  public String forDbFindColumns(String tableName, String columns) {
    return DialectUtils.forDbFindColumns(tableName, columns);
  }
}
