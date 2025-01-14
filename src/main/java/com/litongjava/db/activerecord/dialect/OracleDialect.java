package com.litongjava.db.activerecord.dialect;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Row;
import com.litongjava.db.activerecord.Table;
import com.litongjava.db.activerecord.builder.TimestampProcessedModelBuilder;
import com.litongjava.db.activerecord.builder.TimestampProcessedRecordBuilder;
import com.litongjava.tio.utils.json.Json;

/**
 * OracleDialect.
 */
public class OracleDialect extends Dialect {

  public OracleDialect() {
    this.modelBuilder = TimestampProcessedModelBuilder.me;
    this.recordBuilder = TimestampProcessedRecordBuilder.me;
  }

  public String forTableBuilderDoBuild(String tableName) {
    return "select * from " + tableName + " where rownum < 1";
  }

  // insert into table (id,name) values(seq.nextval, ？)
  public void forModelSave(Table table, Map<String, Object> attrs, StringBuilder sql, List<Object> paras) {
    sql.append("insert into ").append(table.getName()).append('(');
    StringBuilder temp = new StringBuilder(") values(");
    String[] pKeys = table.getPrimaryKey();
    int count = 0;
    for (Entry<String, Object> e : attrs.entrySet()) {
      String colName = e.getKey();
      if (table.hasColumnLabel(colName)) {
        if (count++ > 0) {
          sql.append(", ");
          temp.append(", ");
        }
        sql.append(colName);
        Object value = e.getValue();
        if (value instanceof String && isPrimaryKey(colName, pKeys) && ((String) value).endsWith(".nextval")) {
          temp.append(value);
        } else {
          temp.append('?');
          paras.add(value);
        }
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

  public void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, StringBuilder sql, List<Object> paras) {
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
  
  public String forDbDeleteByField(String tableName, String field) {
    StringBuilder sql = new StringBuilder(45);
    sql.append("delete from ");
    sql.append(tableName);
    sql.append(" where ");
    sql.append(field).append(" = ?");
    return sql.toString();
  }

  public void forDbSave(String tableName, String[] pKeys, Row record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    sql.append("insert into ");
    sql.append(tableName).append('(');
    StringBuilder temp = new StringBuilder();
    temp.append(") values(");

    int count = 0;
    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      String colName = e.getKey();
      if (count++ > 0) {
        sql.append(", ");
        temp.append(", ");
      }
      sql.append(colName);

      Object value = e.getValue();
      if (value instanceof String && isPrimaryKey(colName, pKeys) && ((String) value).endsWith(".nextval")) {
        temp.append(value);
      } else {
        temp.append('?');
        paras.add(value);
      }
    }
    sql.append(temp.toString()).append(')');
  }

  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Row record, StringBuilder sql, List<Object> paras) {
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
    int start = (pageNumber - 1) * pageSize;
    int end = pageNumber * pageSize;
    StringBuilder ret = new StringBuilder();
    ret.append("select * from ( select row_.*, rownum rownum_ from (  ");
    ret.append(findSql);
    ret.append(" ) row_ where rownum <= ").append(end).append(") table_alias");
    ret.append(" where table_alias.rownum_ > ").append(start);
    return ret.toString();
  }

  public boolean isOracle() {
    return true;
  }

  public void fillStatement(PreparedStatement pst, List<Object> paras) throws SQLException {
    for (int i = 0, size = paras.size(); i < size; i++) {
      fillPst(pst, i, paras.get(i));
    }
  }

  public void fillStatement(PreparedStatement pst, Object... paras) throws SQLException {
    for (int i = 0, size = paras.length; i < size; i++) {
      fillPst(pst, i, paras[i]);
    }
  }

  public String getDefaultPrimaryKey() {
    return "ID";
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
  public void forDbDelete(String tableName, String[] pKeys, Row record, StringBuilder sql, List<Object> paras) {
    DialectUtils.forDbDelete(tableName, pKeys, record, sql, paras);
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

  @Override
  public StringBuffer forDbFind(String tableName, String columns, Row record, List<Object> paras) {
    StringBuffer sql = new StringBuffer();
    tableName = tableName.trim();
    sql.append("select ").append(columns).append(" from \"").append(tableName).append("\"");

    if (!record.getColumns().isEmpty()) {
      sql.append(" where ");
      boolean first = true;

      for (Entry<String, Object> e : record.getColumns().entrySet()) {
        if (!first) {
          sql.append(" and ");
        } else {
          first = false;
        }
        sql.append('"').append(e.getKey()).append("\" = ?");
        paras.add(e.getValue());
      }
    }
    return sql;
  }

  @Override
  public StringBuffer forDbFindByField(String tableName, String columns, String field, Object fieldValue, List<Object> paras) {
    StringBuffer sql = new StringBuffer();
    tableName = tableName.trim();
    sql.append("select ").append(columns).append(" from \"").append(tableName).append("\"");
    sql.append(" where ");
    sql.append('"').append(field).append("\" = ?");
    paras.add(fieldValue);
    return sql;
  }

  @Override
  public String forColumns(String columns) {
    return DialectUtils.forColumns(columns);
  }

}
