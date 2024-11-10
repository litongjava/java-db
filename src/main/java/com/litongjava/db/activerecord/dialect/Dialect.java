package com.litongjava.db.activerecord.dialect;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.litongjava.db.activerecord.Config;
import com.litongjava.db.activerecord.Model;
import com.litongjava.db.activerecord.ModelBuilder;
import com.litongjava.db.activerecord.Record;
import com.litongjava.db.activerecord.RecordBuilder;
import com.litongjava.db.activerecord.Table;
import com.litongjava.db.activerecord.builder.KeepByteAndShortModelBuilder;
import com.litongjava.db.activerecord.builder.KeepByteAndShortRecordBuilder;
import com.litongjava.model.page.Page;

/**
 * Dialect.
 */
public abstract class Dialect {

  // 指示 Generator、ModelBuilder、RecordBuilder 是否保持住 Byte、Short 类型
  protected boolean keepByteAndShort = false;
  protected ModelBuilder modelBuilder = ModelBuilder.me;
  protected RecordBuilder recordBuilder = RecordBuilder.me;

  // Methods for common
  public abstract String forTableBuilderDoBuild(String tableName);

  public abstract String forPaginate(int pageNumber, int pageSize, StringBuilder findSql);

  // Methods for Model
  public abstract String forModelFindById(Table table, String columns);

  public abstract String forModelDeleteById(Table table);

  public abstract void forModelSave(Table table, Map<String, Object> attrs, StringBuilder sql, List<Object> paras);

  public abstract void forModelUpdate(Table table, Map<String, Object> attrs, Set<String> modifyFlag, StringBuilder sql, List<Object> paras);

  // Methods for DbPro. Do not delete the String[] pKeys parameter, the element of pKeys needs to trim()
  public abstract String forDbFindById(String tableName, String[] pKeys);

  public abstract String forDbFindColumnsById(String tableName, String columns, String[] pKeys);

  public abstract String forDbFindColumns(String tableName, String columns);

  public abstract String forDbDeleteById(String tableName, String[] pKeys);

  public abstract void forDbSave(String tableName, String[] pKeys, Record record, StringBuilder sql, List<Object> paras);

  public abstract void forDbDelete(String tableName, String[] pKeys, Record record, StringBuilder sql, List<Object> paras);

  public abstract void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Record record, StringBuilder sql, List<Object> paras);

  public abstract void forDbUpdate(String tableName, String[] pKeys, Object[] ids, Record record, StringBuilder sql, List<Object> paras, String[] jsonFields);

  public abstract String forExistsByFields(String tableName, String fields);

  public abstract void transformJsonFields(Record record, String[] jsonFields);

  public abstract void transformJsonFields(List<Record> modelOrRecordList, String[] jsonFields);

  public String forFindAll(String tableName) {
    return "select * from " + tableName;
  }

  /**
   * 指示 Generator、ModelBuilder、RecordBuilder 是否保持住 Byte、Short 类型
   */
  public Dialect setKeepByteAndShort(boolean keepByteAndShort) {
    this.keepByteAndShort = keepByteAndShort;
    /**
     * 内部的 4 个 if 判断是为了避免替换掉用户通过 setModelBuilder(...)
     * setRecordBuilder(...) 配置的自定义 builder
     */
    if (keepByteAndShort) {
      if (modelBuilder.getClass() == ModelBuilder.class) {
        modelBuilder = KeepByteAndShortModelBuilder.me;
      }
      if (recordBuilder.getClass() == RecordBuilder.class) {
        recordBuilder = KeepByteAndShortRecordBuilder.me;
      }
    } else {
      if (modelBuilder.getClass() == KeepByteAndShortModelBuilder.class) {
        modelBuilder = ModelBuilder.me;
      }
      if (recordBuilder.getClass() == KeepByteAndShortRecordBuilder.class) {
        recordBuilder = RecordBuilder.me;
      }
    }
    return this;
  }

  /**
   * 指示 MetaBuilder 生成的 ColumnMeta.javaType 是否保持住 Byte、Short 类型
   * 进而 BaseModelBuilder 生成针对 Byte、Short 类型的获取方法：
   * getByte(String)、getShort(String)
   */
  public boolean isKeepByteAndShort() {
    return keepByteAndShort;
  }

  /**
   * 配置自定义 ModelBuilder
   * <p>
   * 通过继承扩展 ModelBuilder 可以对 JDBC 到 java 数据类型进行定制化转换
   * 不同数据库从 JDBC 到 java 数据类型的映射关系有所不同
   * <p>
   * 此外，还可以通过改变 ModelBuilder.buildLabelNamesAndTypes()
   * 方法逻辑，实现下划线字段名转驼峰变量名的功能
   */
  public Dialect setModelBuilder(ModelBuilder modelBuilder) {
    this.modelBuilder = modelBuilder;
    return this;
  }

  /**
   * 配置自定义 RecordBuilder
   * <p>
   * 通过继承扩展 RecordBuilder 可以对 JDBC 到 java 数据类型进行定制化转换
   * 不同数据库从 JDBC 到 java 数据类型的映射关系有所不同
   * <p>
   * 此外，还可以通过改变 RecordBuilder.buildLabelNamesAndTypes()
   * 方法逻辑，实现下划线字段名转驼峰变量名的功能
   */
  public Dialect setRecordBuilder(RecordBuilder recordBuilder) {
    this.recordBuilder = recordBuilder;
    return this;
  }

  @SuppressWarnings("rawtypes")
  public <T> List<T> buildModelList(ResultSet rs, Class<? extends Model> modelClass) throws SQLException, ReflectiveOperationException {
    return modelBuilder.build(rs, modelClass);
  }

  @SuppressWarnings("rawtypes")
  public <T> void eachModel(ResultSet rs, Class<? extends Model> modelClass, Function<T, Boolean> func) throws SQLException, ReflectiveOperationException {
    modelBuilder.build(rs, modelClass, func);
  }

  public List<Record> buildRecordList(Config config, ResultSet rs) throws SQLException {
    return recordBuilder.build(config, rs);
  }

  public List<Record> buildRecordListWithJsonFields(Config config, ResultSet rs, String[] jsonFields) throws SQLException {
    return recordBuilder.buildJsonFields(config, rs, jsonFields);
  }

  public void eachRecord(Config config, ResultSet rs, Function<Record, Boolean> func) throws SQLException {
    recordBuilder.build(config, rs, func);
  }

  /**
   * 用于获取 Model.save() 以后自动生成的主键值，可通过覆盖此方法实现更精细的控制
   * 目前只有 PostgreSqlDialect，覆盖过此方法
   */
  public void getModelGeneratedKey(Model<?> model, PreparedStatement pst, Table table) throws SQLException {
    String[] pKeys = table.getPrimaryKey();
    ResultSet rs = pst.getGeneratedKeys();
    for (String pKey : pKeys) {
      if (model.get(pKey) == null || isOracle()) {
        if (rs.next()) {
          Class<?> colType = table.getColumnType(pKey);
          if (colType != null) { // 支持没有主键的用法，有人将 model 改造成了支持无主键:济南-费小哥
            if (colType == Integer.class || colType == int.class) {
              model.set(pKey, rs.getInt(1));
            } else if (colType == Long.class || colType == long.class) {
              model.set(pKey, rs.getLong(1));
            } else if (colType == BigInteger.class) {
              processGeneratedBigIntegerKey(model, pKey, rs.getObject(1));
            } else {
              model.set(pKey, rs.getObject(1)); // It returns Long for int colType for mysql
            }
          }
        }
      }
    }
    rs.close();
  }

  /**
   * mysql 数据库的  bigint unsigned 对应的 java 类型为 BigInteger
   * 但是 rs.getObject(1) 返回值为 Long 型，造成 model.save() 以后
   * model.getId() 时的类型转换异常
   */
  protected void processGeneratedBigIntegerKey(Model<?> model, String pKey, Object v) {
    if (v instanceof BigInteger) {
      model.set(pKey, (BigInteger) v);
    } else if (v instanceof Number) {
      Number n = (Number) v;
      model.set(pKey, BigInteger.valueOf(n.longValue()));
    } else {
      model.set(pKey, v);
    }
  }

  /**
   * 用于获取 Db.save(tableName, record) 以后自动生成的主键值，可通过覆盖此方法实现更精细的控制
   * 目前只有 PostgreSqlDialect，覆盖过此方法
   */
  public void getRecordGeneratedKey(PreparedStatement pst, Record record, String[] pKeys) throws SQLException {
    ResultSet rs = pst.getGeneratedKeys();
    for (String pKey : pKeys) {
      if (record.get(pKey) == null || isOracle()) {
        if (rs.next()) {
          record.set(pKey, rs.getObject(1)); // It returns Long for int colType for mysql
        }
      }
    }
    rs.close();
  }

  public boolean isOracle() {
    return false;
  }

  public boolean isTakeOverDbPaginate() {
    return false;
  }

  public <T> Page<T> takeOverDbPaginate(Connection conn, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, StringBuilder findSql, Object... paras) throws SQLException {
    throw new RuntimeException("You should implements this method in " + getClass().getName());
  }

  public boolean isTakeOverModelPaginate() {
    return false;
  }

  @SuppressWarnings("rawtypes")
  public Page takeOverModelPaginate(Connection conn, Class<? extends Model> modelClass, int pageNumber, int pageSize, Boolean isGroupBySql, String totalRowSql, StringBuilder findSql, Object... paras)
      throws Exception {
    throw new RuntimeException("You should implements this method in " + getClass().getName());
  }

  public void fillStatement(PreparedStatement pst, List<Object> paras) throws SQLException {
    for (int i = 0, size = paras.size(); i < size; i++) {
      pst.setObject(i + 1, paras.get(i));
    }
  }

  public void fillStatement(PreparedStatement pst, Object... paras) throws SQLException {
    for (int i = 0; i < paras.length; i++) {
      pst.setObject(i + 1, paras[i]);
    }
  }

  public String getDefaultPrimaryKey() {
    return "id";
  }

  public boolean isPrimaryKey(String colName, String[] pKeys) {
    for (String pKey : pKeys) {
      if (colName.equalsIgnoreCase(pKey)) {
        return true;
      }
    }
    return false;
  }

  protected static class Holder {
    // "order\\s+by\\s+[^,\\s]+(\\s+asc|\\s+desc)?(\\s*,\\s*[^,\\s]+(\\s+asc|\\s+desc)?)*";
    private static final Pattern ORDER_BY_PATTERN = Pattern.compile("order\\s+by\\s+[^,\\s]+(\\s+asc|\\s+desc)?(\\s*,\\s*[^,\\s]+(\\s+asc|\\s+desc)?)*", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
  }

  public String replaceOrderBy(String sql) {
    return Holder.ORDER_BY_PATTERN.matcher(sql).replaceAll("");
  }

  /**
   * fillStatement 时处理日期类型和更多类型
   */
  protected void fillStatementHandleDateType(PreparedStatement pst, List<Object> paras) throws SQLException {
    for (int i = 0, size = paras.size(); i < size; i++) {
      Object value = paras.get(i);
      if (value instanceof java.util.Date) {
        if (value instanceof java.sql.Date) {
          pst.setDate(i + 1, (java.sql.Date) value);
        } else if (value instanceof java.sql.Timestamp) {
          pst.setTimestamp(i + 1, (java.sql.Timestamp) value);
        } else {
          // Oracle、SqlServer 中的 TIMESTAMP、DATE 支持 new Date() 给值
          java.util.Date d = (java.util.Date) value;
          pst.setTimestamp(i + 1, new java.sql.Timestamp(d.getTime()));
        }
      } else if (value instanceof Long) {
        pst.setLong(i + 1, (Long) value);
      } else if (value instanceof Integer) {
        pst.setInt(i + 1, (Integer) value);
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
      } else {
        pst.setObject(i + 1, value); // Fallback to default handling
      }
    }
  }

  /**
   * fillStatement 时处理日期类型
   */
  protected void fillStatementHandleDateType(PreparedStatement pst, Object... paras) throws SQLException {
    for (int i = 0; i < paras.length; i++) {
      Object value = paras[i];
      if (value instanceof java.util.Date) {
        if (value instanceof java.sql.Date) {
          pst.setDate(i + 1, (java.sql.Date) value);
        } else if (value instanceof java.sql.Timestamp) {
          pst.setTimestamp(i + 1, (java.sql.Timestamp) value);
        } else {
          // Oracle、SqlServer 中的 TIMESTAMP、DATE 支持 new Date() 给值
          java.util.Date d = (java.util.Date) value;
          pst.setTimestamp(i + 1, new java.sql.Timestamp(d.getTime()));
        }
      } else {
        pst.setObject(i + 1, value);
      }
    }
  }

  /**
   * 为分页方法生成查询 totalRow 值的 sql
   *
   * @param select          sql 语句的 select 部分
   * @param sqlExceptSelect sql 语句除了 select 以外的部分
   * @param ext             扩展参数，在 Model 调用时传入 Model 对象，在 DbPro 调用时传入 null
   */
  public String forPaginateTotalRow(String select, String sqlExceptSelect, Object ext) {
    return "select count(*) " + replaceOrderBy(sqlExceptSelect);
  }

  public void trimPrimaryKeys(String[] pKeys) {
    DialectUtils.trimPrimaryKeys(pKeys);
  }

}
