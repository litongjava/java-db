package com.litongjava.db.base;

import java.util.List;

import org.postgresql.util.PGobject;

import com.litongjava.db.activerecord.Db;
import com.litongjava.db.activerecord.Row;
import com.litongjava.db.SqlPara;
import com.litongjava.model.page.Page;

public abstract class DbBase {

  /**
   * 子类实现：返回当前 Dao 对应表名
   */
  public abstract String getTableName();

  /* ===================== save 系列 ===================== */

  public boolean save(Row record) {
    return Db.save(getTableName(), record);
  }

  public boolean save(String primaryKey, Row record) {
    return Db.save(getTableName(), primaryKey, record);
  }

  public boolean saveIfAbsent(Row record) {
    return Db.saveIfAbset(getTableName(), record);
  }

  public boolean save(Row record, String[] jsonFields) {
    return Db.save(getTableName(), record, jsonFields);
  }

  public boolean save(String primaryKey, Row record, String[] jsonFields) {
    return Db.save(getTableName(), primaryKey, record, jsonFields);
  }

  /* ===================== update 系列 ===================== */

  /**
   * 使用 Db.update(String tableName, Row record)
   * tableName 由 getTableName 提供
   */
  public boolean update(Row record) {
    return Db.update(getTableName(), record);
  }

  public boolean update(String primaryKeys, Row record) {
    return Db.update(getTableName(), primaryKeys, record);
  }

  public boolean update(String primaryKey, Row record, String[] jsonFields) {
    return Db.update(getTableName(), primaryKey, record, jsonFields);
  }

  /* ===================== delete 系列 ===================== */

  public boolean deleteById(Object idValue) {
    return Db.deleteById(getTableName(), idValue);
  }

  public boolean deleteById(String primaryKey, Object idValue) {
    return Db.deleteById(getTableName(), primaryKey, idValue);
  }

  public boolean deleteByIds(String primaryKey, Object... idValues) {
    return Db.deleteByIds(getTableName(), primaryKey, idValues);
  }

  public boolean delete(Row record) {
    return Db.delete(getTableName(), record);
  }

  public boolean delete(String primaryKey, Row record) {
    return Db.delete(getTableName(), primaryKey, record);
  }

  public boolean deleteByIds(Row record) {
    return Db.deleteByIds(getTableName(), record);
  }

  /* ===================== query*ById / ByField ===================== */

  public <T> T queryColumnById(String column, Object id) {
    return Db.queryColumnById(getTableName(), column, id);
  }

  public Long queryLongById(String column, Object id) {
    return Db.queryLongById(getTableName(), column, id);
  }

  public Long queryStrById(String column, Object id) {
    return Db.queryStrById(getTableName(), column, id);
  }

  public <T> T queryColumnByField(String column, String field, Object value) {
    return Db.queryColumnByField(getTableName(), column, field, value);
  }

  public PGobject queryPGobjectById(String column, Object id) {
    return Db.queryPGobjectById(getTableName(), column, id);
  }

  /* ===================== find 系列（按表） ===================== */

  public List<Row> find(Row where) {
    return Db.find(getTableName(), where);
  }

  public List<Row> find(String columns, Row where) {
    return Db.find(getTableName(), columns, where);
  }

  public List<Row> findByField(String field, Object value) {
    return Db.findByField(getTableName(), field, value);
  }

  public List<Row> findIn(String primaryKey, Object... ids) {
    return Db.findIn(getTableName(), primaryKey, ids);
  }

  public List<Row> findColumnsIn(String columns, String primaryKey, Object... ids) {
    return Db.findColumnsIn(getTableName(), columns, primaryKey, ids);
  }

  public List<Row> findColumnsIn(String columns, String primaryKey, List<?> ids) {
    return Db.findColumnsIn(getTableName(), columns, primaryKey, ids);
  }

  public List<Row> findAll() {
    return Db.findAll(getTableName());
  }

  public List<Row> findColumns(String columns) {
    return Db.findColumns(getTableName(), columns);
  }

  public Row findFirst(Row where) {
    return Db.findFirst(getTableName(), where);
  }

  public Row findFirst(String columns, Row where) {
    return Db.findFirst(getTableName(), columns, where);
  }

  public Row findById(Object id) {
    return Db.findById(getTableName(), id);
  }

  public Row findById(String primaryKey, Object id) {
    return Db.findById(getTableName(), primaryKey, id);
  }

  public <T> T findById(Class<T> clazz, Object id) {
    // 使用 Db.findById(Class<T> clazz, String tableName, Object idValue)
    return Db.findById(clazz, getTableName(), id);
  }

  public <T> T findById(Class<T> clazz, String primaryKey, Object id) {
    return Db.findById(clazz, getTableName(), primaryKey, id);
  }

  public Row findColumnsById(String columns, Object id) {
    return Db.findColumnsById(getTableName(), columns, id);
  }

  public Row findColumnsById(String columns, String primaryKey, Object id) {
    return Db.findColumnsById(getTableName(), columns, primaryKey, id);
  }

  public Row findByIds(String primaryKey, Object... ids) {
    return Db.findByIds(getTableName(), primaryKey, ids);
  }

  public <T> T findByIds(Class<T> clazz, String primaryKey, Object... ids) {
    return Db.findByIds(clazz, getTableName(), primaryKey, ids);
  }

  public Row findColumnsByIds(String columns, String primaryKey, Object... ids) {
    return Db.findColumnsByIds(getTableName(), columns, primaryKey, ids);
  }

  public List<Row> findByColumn(String column, Object value) {
    return Db.findByColumn(getTableName(), column, value);
  }

  /* ===================== exists / count ===================== */

  public boolean exists(String fields, Object... paras) {
    return Db.exists(getTableName(), fields, paras);
  }

  public Long count() {
    return Db.countTable(getTableName());
  }

  /* ===================== paginate / SqlPara 相关（按需封装） ===================== */
  /**
   * 下面这些分页方法本身不带 tableName 参数，通常在 sql 里直接写表名。
   * 为了统一调用出口，也可以在 DbBase 再包一层，视你项目风格决定是否使用。
   * 这里给出一个示例：SqlPara 直接转发给 Db。
   */

  public Page<Row> paginate(int pageNumber, int pageSize, SqlPara sqlPara) {
    return Db.paginate(pageNumber, pageSize, sqlPara);
  }

  public Page<Row> paginate(int pageNumber, int pageSize, boolean isGroupBySql, SqlPara sqlPara) {
    return Db.paginate(pageNumber, pageSize, isGroupBySql, sqlPara);
  }

  public Page<Row> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) {
    return Db.paginate(pageNumber, pageSize, select, sqlExceptSelect, paras);
  }

  public Page<Row> paginate(int pageNumber, int pageSize, String select, String sqlExceptSelect) {
    return Db.paginate(pageNumber, pageSize, select, sqlExceptSelect);
  }

  /* ===================== Sql 模板相关（可选） ===================== */

  public List<Row> findByTemplate(String key, Object... paras) {
    return Db.template(key, paras).find();
  }

  public Row findFirstByTemplate(String key, Object... paras) {
    return Db.template(key, paras).findFirst();
  }

  public Page<Row> paginateByTemplate(int pageNumber, int pageSize, String key, Object... paras) {
    return Db.template(key, paras).paginate(pageNumber, pageSize);
  }
}