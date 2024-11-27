package com.litongjava.db.activerecord.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.litongjava.db.activerecord.ActiveRecordException;
import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Model;
import com.litongjava.db.activerecord.ModelBuilder;
import com.litongjava.db.activerecord.Row;
import com.litongjava.db.activerecord.Table;
import com.litongjava.db.activerecord.builder.TimestampProcessedModelBuilder;
import com.litongjava.db.activerecord.builder.TimestampProcessedRecordBuilder;
import com.litongjava.model.page.Page;
import com.litongjava.tio.utils.json.Json;

/**
 * AnsiSqlDialect. Try to use ANSI SQL dialect with ActiveRecordPlugin.
 * <p>
 * A clever person solves a problem. A wise person avoids it.
 */
public class AnsiSqlDialect extends Dialect {

  public AnsiSqlDialect() {
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
   * SELECT * FROM subject t1 WHERE (SELECT count(*) FROM subject t2 WHERE t2.id < t1.id AND t2.key = '123') > = 10 AND (SELECT count(*) FROM subject t2 WHERE t2.id < t1.id AND t2.key = '123') < 20 AND t1.key = '123'
   */
  public String forPaginate(int pageNumber, int pageSize, StringBuilder findSql) {
    throw new ActiveRecordException("Your should not invoke this method because takeOverDbPaginate(...) will take over it.");
  }

  public boolean isTakeOverDbPaginate() {
    return true;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Page<Row> takeOverDbPaginate(Connection conn, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, StringBuilder findSql, Object... paras) throws SQLException {
    // String totalRowSql = "select count(*) " + replaceOrderBy(sqlExceptSelect);
    List result = CPI.query(conn, totalRowSql, paras);
    int size = result.size();
    if (isGroupBySql == null) {
      isGroupBySql = size > 1;
    }

    long totalRow;
    if (isGroupBySql) {
      totalRow = size;
    } else {
      totalRow = (size > 0) ? ((Number) result.get(0)).longValue() : 0;
    }
    if (totalRow == 0) {
      return new Page<Row>(new ArrayList<Row>(0), pageNumber, pageSize, 0, 0);
    }

    int totalPage = (int) (totalRow / pageSize);
    if (totalRow % pageSize != 0) {
      totalPage++;
    }
    if (pageNumber > totalPage) {
      return new Page<Row>(new ArrayList<Row>(0), pageNumber, pageSize, totalPage, (int) totalRow);
    }

    // StringBuilder sql = new StringBuilder();
    // sql.append(select).append(" ").append(sqlExceptSelect);
    PreparedStatement pst = conn.prepareStatement(findSql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    for (int i = 0; i < paras.length; i++) {
      pst.setObject(i + 1, paras[i]);
    }
    ResultSet rs = pst.executeQuery();

    // move the cursor to the start
    int offset = pageSize * (pageNumber - 1);
    for (int i = 0; i < offset; i++) {
      if (!rs.next()) {
        break;
      }
    }

    List<Row> list = buildRecord(rs, pageSize);
    if (rs != null)
      rs.close();
    if (pst != null)
      pst.close();
    return new Page<Row>(list, pageNumber, pageSize, totalPage, (int) totalRow);
  }

  private List<Row> buildRecord(ResultSet rs, int pageSize) throws SQLException {
    List<Row> result = new ArrayList<Row>();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    String[] labelNames = new String[columnCount + 1];
    int[] types = new int[columnCount + 1];
    buildLabelNamesAndTypes(rsmd, labelNames, types);
    for (int k = 0; k < pageSize && rs.next(); k++) {
      Row record = new Row();
      Map<String, Object> columns = record.getColumns();
      for (int i = 1; i <= columnCount; i++) {
        Object value;
        if (types[i] < Types.BLOB) {
          value = rs.getObject(i);
        } else if (types[i] == Types.CLOB) {
          value = ModelBuilder.me.handleClob(rs.getClob(i));
        } else if (types[i] == Types.NCLOB) {
          value = ModelBuilder.me.handleClob(rs.getNClob(i));
        } else if (types[i] == Types.BLOB) {
          value = ModelBuilder.me.handleBlob(rs.getBlob(i));
        } else {
          value = rs.getObject(i);
        }
        columns.put(labelNames[i], value);
      }
      result.add(record);
    }
    return result;
  }

  private void buildLabelNamesAndTypes(ResultSetMetaData rsmd, String[] labelNames, int[] types) throws SQLException {
    for (int i = 1; i < labelNames.length; i++) {
      // 备忘：getColumnLabel 获取 sql as 子句指定的名称而非字段真实名称
      labelNames[i] = rsmd.getColumnLabel(i);
      types[i] = rsmd.getColumnType(i);
    }
  }

  public boolean isTakeOverModelPaginate() {
    return true;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Page<? extends Model> takeOverModelPaginate(Connection conn, Class<? extends Model> modelClass, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, StringBuilder findSql,
      Object... paras) throws Exception {
    // String totalRowSql = "select count(*) " + replaceOrderBy(sqlExceptSelect);
    List result = CPI.query(conn, totalRowSql, paras);
    int size = result.size();
    if (isGroupBySql == null) {
      isGroupBySql = size > 1;
    }

    long totalRow;
    if (isGroupBySql) {
      totalRow = size;
    } else {
      totalRow = (size > 0) ? ((Number) result.get(0)).longValue() : 0;
    }
    if (totalRow == 0) {
      return new Page(new ArrayList(0), pageNumber, pageSize, 0, 0); // totalRow = 0;
    }

    int totalPage = (int) (totalRow / pageSize);
    if (totalRow % pageSize != 0) {
      totalPage++;
    }
    if (pageNumber > totalPage) {
      return new Page(new ArrayList(0), pageNumber, pageSize, totalPage, (int) totalRow);
    }

    // --------
    // StringBuilder sql = new StringBuilder();
    // sql.append(select).append(" ").append(sqlExceptSelect);
    PreparedStatement pst = conn.prepareStatement(findSql.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    for (int i = 0; i < paras.length; i++) {
      pst.setObject(i + 1, paras[i]);
    }
    ResultSet rs = pst.executeQuery();

    // move the cursor to the start
    int offset = pageSize * (pageNumber - 1);
    for (int i = 0; i < offset; i++) {
      if (!rs.next()) {
        break;
      }
    }

    List list = buildModel(rs, modelClass, pageSize);
    if (rs != null)
      rs.close();
    if (pst != null)
      pst.close();
    return new Page(list, pageNumber, pageSize, totalPage, (int) totalRow);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public final <T> List<T> buildModel(ResultSet rs, Class<? extends Model> modelClass, int pageSize) throws SQLException, ReflectiveOperationException {
    List<T> result = new ArrayList<T>();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    String[] labelNames = new String[columnCount + 1];
    int[] types = new int[columnCount + 1];
    buildLabelNamesAndTypes(rsmd, labelNames, types);
    for (int k = 0; k < pageSize && rs.next(); k++) {
      Model<?> ar = modelClass.newInstance();
      Map<String, Object> attrs = CPI.getAttrs(ar);
      for (int i = 1; i <= columnCount; i++) {
        Object value;
        if (types[i] < Types.BLOB) {
          value = rs.getObject(i);
        } else if (types[i] == Types.CLOB) {
          value = ModelBuilder.me.handleClob(rs.getClob(i));
        } else if (types[i] == Types.NCLOB) {
          value = ModelBuilder.me.handleClob(rs.getNClob(i));
        } else if (types[i] == Types.BLOB) {
          value = ModelBuilder.me.handleBlob(rs.getBlob(i));
        } else {
          value = rs.getObject(i);
        }
        attrs.put(labelNames[i], value);
      }
      result.add((T) ar);
    }
    return result;
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

    // 使用 ANSI SQL 标准，直接拼接表名和列名，不加特殊引用符号
    sql.append("select ").append(columns).append(" from ").append(tableName);

    if (!record.getColumns().isEmpty()) {
      sql.append(" where ");
      boolean first = true;

      for (Entry<String, Object> e : record.getColumns().entrySet()) {
        if (!first) {
          sql.append(" and ");
        } else {
          first = false;
        }
        sql.append(e.getKey()).append(" = ?");
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
