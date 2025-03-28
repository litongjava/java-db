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
 * SqlServerDialect 为OSC 网友战五渣贡献代码：http://www.oschina.net/question/2333909_234198
 */
public class SqlServerDialect extends Dialect {

  public SqlServerDialect() {
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

  /**
   * sql.replaceFirst("(?i)select", "") 正则中带有 "(?i)" 前缀，指定在匹配时不区分大小写
   */
  public String forPaginate(int pageNumber, int pageSize, StringBuilder findSql) {
    int end = pageNumber * pageSize;
    if (end <= 0) {
      end = pageSize;
    }
    int begin = (pageNumber - 1) * pageSize;
    if (begin < 0) {
      begin = 0;
    }
    StringBuilder ret = new StringBuilder();
    ret.append("SELECT * FROM ( SELECT row_number() over (order by tempcolumn) temprownumber, * FROM ");
    ret.append(" ( SELECT TOP ").append(end).append(" tempcolumn=0,");
    ret.append(findSql.toString().replaceFirst("(?i)select", ""));
    ret.append(")vip)mvp where temprownumber>").append(begin);
    return ret.toString();
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

  @Override
  public StringBuffer forDbFind(String tableName, String columns, Row record, List<Object> paras) {
    StringBuffer sql = new StringBuffer();
    tableName = tableName.trim();
    sql.append("select ").append(columns).append(" from [").append(tableName).append("]");

    if (!record.getColumns().isEmpty()) {
      sql.append(" where ");
      boolean first = true;

      for (Entry<String, Object> e : record.getColumns().entrySet()) {
        if (!first) {
          sql.append(" and ");
        } else {
          first = false;
        }
        sql.append('[').append(e.getKey()).append("] = ?");
        paras.add(e.getValue());
      }
    }
    return sql;
  }

  @Override
  public StringBuffer forDbFindByField(String tableName, String columns, String field, Object fieldValue, List<Object> paras) {
    StringBuffer sql = new StringBuffer();
    tableName = tableName.trim();
    sql.append("select ").append(columns).append(" from [").append(tableName).append("]");

    if (field != null && !field.isEmpty()) {
      sql.append(" where ");
      sql.append('[').append(field).append("] = ?");
      paras.add(fieldValue);
    }
    return sql;
  }

  @Override
  public String forColumns(String columns) {
    return DialectUtils.forColumns(columns);
  }

  @Override
  public void forDbSaveIfAbset(String tableName, String[] pKeys, Row record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    // Build columns and values
    StringBuilder insertColumns = new StringBuilder();
    StringBuilder insertValues = new StringBuilder();
    StringBuilder sourceColumns = new StringBuilder();
    StringBuilder onCondition = new StringBuilder();

    int index = 0;
    for (Entry<String, Object> entry : record.getColumns().entrySet()) {
      String col = entry.getKey();
      Object val = entry.getValue();

      if (index++ > 0) {
        insertColumns.append(", ");
        insertValues.append(", ");
        sourceColumns.append(", ");
      }

      insertColumns.append("[").append(col).append("]");
      insertValues.append("source.").append(col);
      sourceColumns.append("? AS ").append(col);
      paras.add(val);
    }

    // Build ON condition based on primary keys
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        onCondition.append(" AND ");
      }
      onCondition.append("target.[").append(pKeys[i]).append("] = source.").append(pKeys[i]);
    }

    // Assemble full MERGE SQL
    sql.append("MERGE INTO [").append(tableName).append("] AS target ").append("USING (SELECT ").append(sourceColumns).append(") AS source ").append("ON ").append(onCondition).append(" ")
        .append("WHEN NOT MATCHED THEN INSERT (").append(insertColumns).append(") ").append("VALUES (").append(insertValues).append(");");
  }

}
