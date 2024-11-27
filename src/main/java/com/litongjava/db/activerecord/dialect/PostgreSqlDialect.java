package com.litongjava.db.activerecord.dialect;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.postgresql.util.PGobject;

import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Model;
import com.litongjava.db.activerecord.Row;
import com.litongjava.db.activerecord.Table;
import com.litongjava.db.activerecord.builder.TimestampProcessedModelBuilder;
import com.litongjava.db.activerecord.builder.TimestampProcessedRecordBuilder;
import com.litongjava.tio.utils.hutool.StrUtil;
import com.litongjava.tio.utils.json.Json;
import com.litongjava.tio.utils.json.JsonUtils;

/**
 * PostgreSqlDialect.
 */
public class PostgreSqlDialect extends Dialect {

  public PostgreSqlDialect() {
    this.modelBuilder = TimestampProcessedModelBuilder.me;
    this.recordBuilder = TimestampProcessedRecordBuilder.me;
  }

  public String forTableBuilderDoBuild(String tableName) {
    return "select * from \"" + tableName + "\" where 1 = 2";
  }

  public String forFindAll(String tableName) {
    return "select * from \"" + tableName + "\"";
  }

  public void forModelSave(Table table, Map<String, Object> attrs, StringBuilder sql, List<Object> paras) {
    sql.append("insert into \"").append(table.getName()).append("\"(");
    StringBuilder temp = new StringBuilder(") values(");
    for (Entry<String, Object> e : attrs.entrySet()) {
      String colName = e.getKey();
      if (table.hasColumnLabel(colName)) {
        if (paras.size() > 0) {
          sql.append(", ");
          temp.append(", ");
        }
        sql.append('\"').append(colName).append('\"');
        temp.append('?');
        paras.add(e.getValue());
      }
    }
    sql.append(temp.toString()).append(')');
  }

  public String forModelDeleteById(Table table) {
    String[] pKeys = table.getPrimaryKey();
    StringBuilder sql = new StringBuilder(45);
    sql.append("delete from \"");
    sql.append(table.getName());
    sql.append("\" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('\"').append(pKeys[i]).append("\" = ?");
    }
    return sql.toString();
  }

  public void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, StringBuilder sql, List<Object> paras) {
    sql.append("update \"").append(table.getName()).append("\" set ");
    String[] pKeys = table.getPrimaryKey();
    for (Entry<String, Object> e : attrs.entrySet()) {
      String colName = e.getKey();
      if (modifyFlag.contains(colName) && !isPrimaryKey(colName, pKeys) && table.hasColumnLabel(colName)) {
        if (paras.size() > 0) {
          sql.append(", ");
        }
        sql.append('\"').append(colName).append("\" = ? ");
        paras.add(e.getValue());
      }
    }
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('\"').append(pKeys[i]).append("\" = ?");
      paras.add(attrs.get(pKeys[i]));
    }
  }

  public String forModelFindById(Table table, String columns) {
    StringBuilder sql = new StringBuilder("select ");
    columns = columns.trim();
    if ("*".equals(columns)) {
      sql.append('*');
    } else {
      String[] arr = columns.split(",");
      for (int i = 0; i < arr.length; i++) {
        if (i > 0) {
          sql.append(',');
        }
        sql.append('\"').append(arr[i].trim()).append('\"');
      }
    }

    sql.append(" from \"");
    sql.append(table.getName());
    sql.append("\" where ");
    String[] pKeys = table.getPrimaryKey();
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('\"').append(pKeys[i]).append("\" = ?");
    }
    return sql.toString();
  }

  public String forDbFindById(String tableName, String[] pKeys) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    StringBuilder sql = new StringBuilder("select * from \"").append(tableName).append("\" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('\"').append(pKeys[i]).append("\" = ?");
    }
    return sql.toString();
  }

  @Override
  public void forDbDelete(String tableName, String[] pKeys, Row record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys); // important

    sql.append("delete from \"");
    sql.append(tableName).append("\"");

    sql.append(" where ");

    int i = 0;
    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('\"').append(e.getKey()).append('\"').append("=? ");
      paras.add(e.getValue());
      i++;
    }
  }

  public String forDbDeleteById(String tableName, String[] pKeys) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    StringBuilder sql = new StringBuilder("delete from \"").append(tableName).append("\" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('\"').append(pKeys[i]).append("\" = ?");
    }
    return sql.toString();
  }

  public void forDbSave(String tableName, String[] pKeys, Row record, StringBuilder sql, List<Object> paras) {
    tableName = tableName.trim();
    trimPrimaryKeys(pKeys);

    sql.append("insert into \"");
    sql.append(tableName).append("\"(");
    StringBuilder temp = new StringBuilder();
    temp.append(") values(");

    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      if (paras.size() > 0) {
        sql.append(", ");
        temp.append(", ");
      }
      sql.append('\"').append(e.getKey()).append('\"');
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

    sql.append("update \"").append(tableName).append("\" set ");
    for (Entry<String, Object> e : record.getColumns().entrySet()) {
      String colName = e.getKey();
      if (modifyFlag.contains(colName) && !isPrimaryKey(colName, pKeys)) {
        if (paras.size() > 0) {
          sql.append(", ");
        }
        sql.append('\"').append(colName).append("\" = ? ");
        paras.add(e.getValue());
      }
    }
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('\"').append(pKeys[i]).append("\" = ?");
      paras.add(ids[i]);
    }
  }

  public String forPaginate(int pageNumber, int pageSize, StringBuilder findSql) {
    int offset = pageSize * (pageNumber - 1);
    findSql.append(" limit ").append(pageSize).append(" offset ").append(offset);
    return findSql.toString();
  }

  public void fillStatement(PreparedStatement pst, List<Object> paras) throws SQLException {
    for (int i = 0, size = paras.size(); i < size; i++) {
      Object value = paras.get(i);
      if (value != null) {
        fillPst(pst, i, value);
      } else {
        pst.setObject(i + 1, value);
      }
    }
  }

  public void fillStatement(PreparedStatement pst, Object... paras) throws SQLException {
    for (int i = 0, size = paras.length; i < size; i++) {
      if (paras[i] != null) {
        fillPst(pst, i, paras[i]);
      } else {
        pst.setObject(i + 1, paras[i]);
      }

    }
  }

  /**
   * 解决 PostgreSql 获取自增主键时 rs.getObject(1) 总是返回第一个字段的值，而非返回了 id 值 issue:
   * https://www.oschina.net/question/2312705_2243354
   * <p>
   * 相对于 Dialect 中的默认实现，仅将 rs.getXxx(1) 改成了 rs.getXxx(pKey)
   */
  public void getModelGeneratedKey(Model<?> model, PreparedStatement pst, Table table) throws SQLException {
    String[] pKeys = table.getPrimaryKey();
    ResultSet rs = pst.getGeneratedKeys();
    for (String pKey : pKeys) {
      if (model.get(pKey) == null || isOracle()) {
        if (rs.next()) {
          Class<?> colType = table.getColumnType(pKey);
          if (colType != null) {
            if (colType == Integer.class || colType == int.class) {
              model.set(pKey, rs.getInt(pKey));
            } else if (colType == Long.class || colType == long.class) {
              model.set(pKey, rs.getLong(pKey));
            } else if (colType == BigInteger.class) {
              processGeneratedBigIntegerKey(model, pKey, rs.getObject(pKey));
            } else {
              model.set(pKey, rs.getObject(pKey));
            }
          }
        }
      }
    }
    rs.close();
  }

  /**
   * 解决 PostgreSql 获取自增主键时 rs.getObject(1) 总是返回第一个字段的值，而非返回了 id 值 issue:
   * https://www.oschina.net/question/2312705_2243354
   * <p>
   * 相对于 Dialect 中的默认实现，仅将 rs.getXxx(1) 改成了 rs.getXxx(pKey)
   */
  public void getRecordGeneratedKey(PreparedStatement pst, Row record, String[] pKeys) {
    try (ResultSet rs = pst.getGeneratedKeys()) { // Automatically closes ResultSet
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      if (rs.next()) {
        for (int i = 1; i <= columnCount; i++) {
          String name = metaData.getColumnName(i);
          int columnType = metaData.getColumnType(i);

          Object value;
          switch (columnType) {
          case java.sql.Types.SMALLINT:
            value = rs.getShort(i);
            break;
          case java.sql.Types.INTEGER:
            value = rs.getInt(i);
            break;
          default:
            value = rs.getObject(i);
            break;
          }

          record.set(name, value);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String forDbFindColumnsById(String tableName, String columns, String[] pKeys) {
    StringBuilder sql = forDbFindColumnsReturnStrinBuilder(tableName, columns);
    sql.append(" where ");
    for (int i = 0; i < pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('"').append(pKeys[i]).append("\" = ?");
    }
    return sql.toString();
  }
  
  public static StringBuilder forDbFindColumnsReturnStrinBuilder(String tableName, String columns) {
    StringBuilder sql = new StringBuilder("select ");
    columns = columns.trim();
    if ("*".equals(columns)) {
      sql.append('*');
    } else {
      String[] arr = columns.split(",");
      for (int i = 0; i < arr.length; i++) {
        if (i > 0) {
          sql.append(',');
        }
        sql.append('"').append(arr[i].trim()).append('"');
      }
    }

    sql.append(" from \"").append(tableName).append("\"");
    return sql;
  }

  @Override
  public String forDbFindColumns(String tableName, String columns) {
    StringBuilder sql = new StringBuilder("select ");
    columns = columns.trim();
    if ("*".equals(columns)) {
      sql.append('*');
    } else {
      String[] arr = columns.split(",");
      for (int i = 0; i < arr.length; i++) {
        if (i > 0) {
          sql.append(',');
        }
        sql.append('"').append(arr[i].trim()).append('"');
      }
    }

    sql.append(" from \"").append(tableName).append("\"");
    return sql.toString();
  }

  @Override
  public String forExistsByFields(String tableName, String fields) {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("select count(1) from \"").append(tableName).append("\"");
    String[] split = fields.split(",");
    if (split.length > 0) {
      stringBuffer.append(" where ");
      for (int i = 0; i < split.length; i++) {
        stringBuffer.append('\"').append(split[i].trim()).append('\"').append("= ?");
        if (i < split.length - 1) {
          stringBuffer.append(" AND ");
        }
      }
    }
    return stringBuffer.toString();
  }

  @Override
  public void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Row record, StringBuilder sql, List<Object> paras, String[] jsonFields) {
    if (jsonFields != null && jsonFields.length > 0) {
      for (String f : jsonFields) {
        Object object = record.get(f);
        if (object != null) {
          PGobject pGobject = new PGobject();
          pGobject.setType("jsonb");
          String jsonString = null;
          if (object instanceof String) {
            jsonString = (String) object;
            if ("".equals(jsonString)) {
              try {
                pGobject.setValue(null);
                record.set(f, pGobject);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            } else if (jsonString.startsWith("\"") && jsonString.endsWith("\"")) {
              try {
                pGobject.setValue(jsonString);
                record.set(f, pGobject);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            } else if (jsonString.startsWith("{") && jsonString.endsWith("}")) {
              try {
                pGobject.setValue(jsonString);
                record.set(f, pGobject);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }

            } else if (jsonString.startsWith("[") && jsonString.endsWith("]")) {
              try {
                pGobject.setValue(jsonString);
                record.set(f, pGobject);
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }

          } else {
            jsonString = Json.getJson().toJson(object);
            try {
              pGobject.setValue(jsonString);
              record.set(f, pGobject);
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

        }
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
          PGobject pGobject = new PGobject();
          pGobject.setType("jsonb");
          String jsonString = null;
          if (object instanceof String) {
            jsonString = (String) object;
          } else {
            jsonString = Json.getJson().toJson(object);
          }
          try {
            pGobject.setValue(jsonString);
            record.set(f, pGobject);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
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
            PGobject pGobject = new PGobject();
            pGobject.setType("jsonb");
            String jsonString = null;
            if (object instanceof String) {
              jsonString = (String) object;
            } else {
              jsonString = Json.getJson().toJson(object);
            }
            try {
              pGobject.setValue(jsonString);
              record.set(f, pGobject);
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
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
    } else if (value instanceof String[]) {
      Array sqlArray = pst.getConnection().createArrayOf("varchar", (String[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof int[] || value instanceof Integer[]) {
      Array sqlArray = pst.getConnection().createArrayOf("integer", (Integer[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof long[] || value instanceof Long[]) {
      Array sqlArray = pst.getConnection().createArrayOf("bigint", (Long[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof double[] || value instanceof Double[]) {
      Array sqlArray = pst.getConnection().createArrayOf("float8", (Double[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof float[] || value instanceof Float[]) {
      Array sqlArray = pst.getConnection().createArrayOf("real", (Float[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof boolean[] || value instanceof Boolean[]) {
      Array sqlArray = pst.getConnection().createArrayOf("boolean", (Boolean[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof short[] || value instanceof Short[]) {
      Array sqlArray = pst.getConnection().createArrayOf("smallint", (Short[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof Object[]) {
      Array sqlArray = pst.getConnection().createArrayOf("text", (Object[]) value);
      pst.setArray(i + 1, sqlArray);
    } else if (value instanceof List<?>) {
      // Assuming list of strings; adjust type as needed
      Array sqlArray = pst.getConnection().createArrayOf("text", ((List<?>) value).toArray());
      pst.setArray(i + 1, sqlArray);

    } else if (value instanceof PGobject) {
      pst.setObject(i + 1, value);
    } else {
      // Assume it's an entity, convert to JSON and store as jsonb
      String json = JsonUtils.toJson(value);
      PGobject pgObject = new PGobject();
      pgObject.setType("jsonb");
      pgObject.setValue(json);
      pst.setObject(i + 1, pgObject);
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
  public String forColumns(String columns) {
    if (StrUtil.isNotEmpty(columns)) {
      String[] arr = columns.split(",");
      StringBuffer sql = new StringBuffer();
      for (int i = 0; i < arr.length; i++) {
        if (i > 0) {
          sql.append(',');
        }
        sql.append('\"').append(arr[i].trim()).append('\"');
      }
      return sql.toString();
    } else {
      return null;
    }
  }
}
