package com.litongjava.db.activerecord;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.jfinal.kit.Kv;
import com.jfinal.kit.TypeKit;
import com.litongjava.db.DbJsonObject;
import com.litongjava.model.db.IRow;

/**
 * Record
 */
public class Row implements IRow<Row>, Serializable {

  private static final long serialVersionUID = 905784513600884082L;
  private Map<String, Object> columns; // = getColumnsMap(); // getConfig().containerFactory.getColumnsMap(); // new
                                       // HashMap<String, Object>();
  private String tableName;

  public String getTableName() {
    return tableName;
  }

  public Row setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  /**
   * Flag of column has been modified. update need this flag
   */
  Set<String> modifyFlag;

  @SuppressWarnings("unchecked")
  Set<String> _getModifyFlag() {
    if (modifyFlag == null) {
      Config config = DbKit.getConfig();
      if (config == null) {
        modifyFlag = DbKit.brokenConfig.containerFactory.getModifyFlagSet();
      } else {
        modifyFlag = config.containerFactory.getModifyFlagSet();
      }
    }
    return modifyFlag;
  }

  void clearModifyFlag() {
    if (modifyFlag != null) {
      modifyFlag.clear();
    }
  }

  /**
   * Set the containerFactory by configName. Only the containerFactory of the
   * config used by Record for getColumnsMap()
   * 
   * @param configName the config name
   */
  public Row setContainerFactoryByConfigName(String configName) {
    Config config = DbKit.getConfig(configName);
    if (config == null) {
      throw new IllegalArgumentException("Config not found: " + configName);
    }

    processColumnsMap(config);
    return this;
  }

  // 用于 RecordBuilder 中注入 Map。也可以通过调用 CPI.setColumnsMap(record, columns) 实现
  void setColumnsMap(Map<String, Object> columns) {
    this.columns = columns;
  }

  @SuppressWarnings("unchecked")
  private void processColumnsMap(Config config) {
    if (columns == null || columns.size() == 0) {
      columns = config.containerFactory.getColumnsMap();
    } else {
      Map<String, Object> columnsOld = columns;
      columns = config.containerFactory.getColumnsMap();
      columns.putAll(columnsOld);
    }
  }

  /**
   * Return columns map.
   */
  @SuppressWarnings("unchecked")
  public Map<String, Object> getColumns() {
    if (columns == null) {
      if (DbKit.config == null) {
        columns = DbKit.brokenConfig.containerFactory.getColumnsMap();
      } else {
        columns = DbKit.config.containerFactory.getColumnsMap();
      }
    }
    return columns;
  }

  /**
   * Set columns value with map.
   * 
   * @param columns the columns map
   */
  public Row setColumns(Map<String, Object> columns) {
    for (Entry<String, Object> e : columns.entrySet()) {
      set(e.getKey(), e.getValue());
    }
    return this;
  }

  /**
   * Set columns value with Record.
   * 
   * @param record the Record object
   */
  public Row setColumns(Row record) {
    return setColumns(record.getColumns());
  }

  /**
   * Set columns value with Model object.
   * 
   * @param model the Model object
   */
  public Row setColumns(Model<?> model) {
    return setColumns(model._getAttrs());
  }

  /**
   * Remove attribute of this record.
   * 
   * @param column the column name of the record
   */
  public Row remove(String column) {
    getColumns().remove(column);
    _getModifyFlag().remove(column);
    return this;
  }

  /**
   * Remove columns of this record.
   * 
   * @param columns the column names of the record
   */
  public Row remove(String... columns) {
    if (columns != null) {
      for (String c : columns) {
        this.getColumns().remove(c);
        this._getModifyFlag().remove(c);
      }
    }
    return this;
  }

  /**
   * Remove columns if it is null.
   */
  public Row removeNullValueColumns() {
    for (java.util.Iterator<Entry<String, Object>> it = getColumns().entrySet().iterator(); it.hasNext();) {
      Entry<String, Object> e = it.next();
      if (e.getValue() == null) {
        it.remove();
        _getModifyFlag().remove(e.getKey());
      }
    }
    return this;
  }

  /**
   * Keep columns of this record and remove other columns.
   * 
   * @param columns the column names of the record
   */
  @SuppressWarnings("unchecked")
  public Row keep(String... columns) {
    if (columns != null && columns.length > 0) {
      Config config = DbKit.getConfig();
      if (config == null) { // 支持无数据库连接场景
        config = DbKit.brokenConfig;
      }
      Map<String, Object> newColumns = config.containerFactory.getColumnsMap();
      Set<String> newModifyFlag = config.containerFactory.getModifyFlagSet();
      for (String c : columns) {
        if (this.getColumns().containsKey(c)) // prevent put null value to the newColumns
          newColumns.put(c, this.columns.get(c));
        if (this._getModifyFlag().contains(c))
          newModifyFlag.add(c);
      }
      this.columns = newColumns;
      this.modifyFlag = newModifyFlag;
    } else {
      this.getColumns().clear();
      this.clearModifyFlag();
    }
    return this;
  }

  /**
   * Keep column of this record and remove other columns.
   * 
   * @param column the column names of the record
   */
  public Row keep(String column) {
    if (getColumns().containsKey(column)) { // prevent put null value to the newColumns
      Object keepIt = getColumns().get(column);
      getColumns().clear();
      getColumns().put(column, keepIt);

      boolean keepFlag = _getModifyFlag().contains(column);
      clearModifyFlag();
      if (keepFlag) {
        _getModifyFlag().add(column);
      }
    } else {
      getColumns().clear();
      clearModifyFlag();
    }
    return this;
  }

  /**
   * Remove all columns of this record.
   */
  public Row clear() {
    getColumns().clear();
    clearModifyFlag();
    return this;
  }

  /**
   * Set column to record.
   * 
   * @param column the column name
   * @param value  the value of the column
   */
  public Row set(String column, Object value) {
    getColumns().put(column, value);
    _getModifyFlag().add(column); // Add modify flag, update() need this flag.
    return this;
  }

  /**
   * Get column of any mysql type
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String column) {
    return (T) getColumns().get(column);
  }

  public String[] getStringArray(String column) {
    Object object = getColumns().get(column);
    if (object != null) {
      return (String[]) object;
    }
    return null;
  }

  /**
   * Get column of any mysql type. Returns defaultValue if null.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String column, Object defaultValue) {
    Object result = getColumns().get(column);
    return (T) (result != null ? result : defaultValue);
  }

  public Object getObject(String column) {
    return getColumns().get(column);
  }

  public Object getObject(String column, Object defaultValue) {
    Object result = getColumns().get(column);
    return result != null ? result : defaultValue;
  }

  // get array
  public Integer[] getArrayInteger(String column) {
    Object result = getColumns().get(column);
    return result != null ? (Integer[]) result : null;
  }

  public Long[] getArrayLong(String column) {
    Object result = getColumns().get(column);
    return result != null ? (Long[]) result : null;
  }

  public String[] getArrayString(String column) {
    Object result = getColumns().get(column);
    return result != null ? (String[]) result : null;
  }

  public List<Long> getListLong(String column) {
    Object result = getColumns().get(column);
    if (result == null) {
      return null;
    }
    Long[] ints = (Long[]) result;
    List<Long> list = new ArrayList<>(ints.length);
    for (int i = 0; i < ints.length; i++) {
      list.add(ints[i]);
    }

    return list;
  }

  public List<String> getListString(String column) {
    Object result = getColumns().get(column);
    if (result == null) {
      return null;
    }
    String[] ints = (String[]) result;
    List<String> list = new ArrayList<>(ints.length);
    for (int i = 0; i < ints.length; i++) {
      list.add(ints[i]);
    }

    return list;
  }

  public List<Integer> getListInteger(String column) {
    Object result = getColumns().get(column);
    if (result == null) {
      return null;
    }
    Integer[] ints = (Integer[]) result;
    List<Integer> list = new ArrayList<>(ints.length);
    for (int i = 0; i < ints.length; i++) {
      list.add(ints[i]);
    }

    return list;
  }

  /**
   * Get column of mysql type: varchar, char, enum, set, text, tinytext,
   * mediumtext, longtext
   */
  public String getStr(String column) {
    // return (String)getColumns().get(column);
    Object s = getColumns().get(column);
    return s != null ? s.toString() : null;
  }

  public String getString(String column) {
    Object s = getColumns().get(column);
    return s != null ? s.toString() : null;
  }

  /**
   * Get column of mysql type: int, integer, tinyint(n) n > 1, smallint, mediumint
   */
  public Integer getInt(String column) {
    // Number n = getNumber(column);
    // return n != null ? n.intValue() : null;
    return TypeKit.toInt(getColumns().get(column));
  }

  /**
   * Get column of mysql type: bigint, unsigned int
   */
  public Long getLong(String column) {
    // Number n = getNumber(column);
    // return n != null ? n.longValue() : null;
    return TypeKit.toLong(getColumns().get(column));
  }

  /**
   * Get column of mysql type: unsigned bigint
   */
  public BigInteger getBigInteger(String column) {
    // return (java.math.BigInteger)getColumns().get(column);
    Object n = getColumns().get(column);
    if (n instanceof BigInteger) {
      return (BigInteger) n;
    }

    // 数据类型 id(19 number)在 Oracle Jdbc 下对应的是 BigDecimal,
    // 但是在 MySql 下对应的是 BigInteger，这会导致在 MySql 下生成的代码无法在 Oracle 数据库中使用
    if (n instanceof BigDecimal) {
      return ((BigDecimal) n).toBigInteger();
    } else if (n instanceof Number) {
      return BigInteger.valueOf(((Number) n).longValue());
    } else if (n instanceof String) {
      return new BigInteger((String) n);
    }

    return (BigInteger) n;
  }

  /**
   * Get column of mysql type: date, year
   */
  public java.util.Date getDate(String column) {
    return TypeKit.toDate(getColumns().get(column));
  }

  public LocalDateTime getLocalDateTime(String column) {
    return TypeKit.toLocalDateTime(getColumns().get(column));
  }

  /**
   * Get column of mysql type: time
   */
  public java.sql.Time getTime(String column) {
    return (java.sql.Time) getColumns().get(column);
  }

  /**
   * Get column of mysql type: timestamp, datetime
   */
  public java.sql.Timestamp getTimestamp(String column) {
    return (java.sql.Timestamp) getColumns().get(column);
  }

  public OffsetDateTime getOffsetDateTime(String column) {
    Timestamp ts = getTimestamp(column);
    if (ts != null) {
      return ts.toInstant().atOffset(ZoneOffset.UTC);
    }
    return null;
  }

  /**
   * Get column of mysql type: real, double
   */
  public Double getDouble(String column) {
    // Number n = getNumber(column);
    // return n != null ? n.doubleValue() : null;
    return TypeKit.toDouble(getColumns().get(column));
  }

  /**
   * Get column of mysql type: float
   */
  public Float getFloat(String column) {
    // Number n = getNumber(column);
    // return n != null ? n.floatValue() : null;
    return TypeKit.toFloat(getColumns().get(column));
  }

  public Short getShort(String column) {
    // Number n = getNumber(column);
    // return n != null ? n.shortValue() : null;
    return TypeKit.toShort(getColumns().get(column));
  }

  public Byte getByte(String column) {
    // Number n = getNumber(column);
    // return n != null ? n.byteValue() : null;
    return TypeKit.toByte(getColumns().get(column));
  }

  /**
   * Get column of mysql type: bit, tinyint(1)
   */
  public Boolean getBoolean(String column) {
    // return (Boolean)getColumns().get(column);
    return TypeKit.toBoolean(getColumns().get(column));
  }

  /**
   * Get column of mysql type: decimal, numeric
   */
  public BigDecimal getBigDecimal(String column) {
    return TypeKit.toBigDecimal(getColumns().get(column));
  }

  /**
   * Get column of mysql type: binary, varbinary, tinyblob, blob, mediumblob,
   * longblob I have not finished the test.
   */
  public byte[] getBytes(String column) {
    return (byte[]) getColumns().get(column);
  }

  /**
   * Get column of any type that extends from Number
   */
  public Number getNumber(String column) {
    // return (Number)getColumns().get(column);
    return TypeKit.toNumber(getColumns().get(column));
  }

  @SuppressWarnings("unchecked")
  public <K, V> Map<K, V> getMap(String column) {
    Object object = getColumns().get(column);
    if (object instanceof Map) {
      return (Map<K, V>) object;
    } else {
      throw new RuntimeException(column + " is not type of map");
    }
  }

  public DbJsonObject getJsonObject(String column) {
    Object object = getColumns().get(column);
    if (object != null) {
      return new DbJsonObject(object.toString());
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public <T> T getAs(String column) {
    Object object = getColumns().get(column);
    return (T) object;
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> getList(String column) {
    Object object = getColumns().get(column);
    if (object instanceof List) {
      return (List<T>) object;
    } else {
      throw new RuntimeException(column + " is not type of list");
    }
  }

  public String toString() {
    if (columns == null) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    boolean first = true;
    for (Entry<String, Object> e : getColumns().entrySet()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      Object value = e.getValue();
      if (value != null) {
        value = value.toString();
      }
      sb.append(e.getKey()).append(':').append(value);
    }
    sb.append('}');
    return sb.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof Row))
      return false;
    if (o == this)
      return true;
    return getColumns().equals(((Row) o).getColumns());
  }

  public int hashCode() {
    return getColumns().hashCode();
  }

  /**
   * Return column names of this record.
   */
  public String[] getColumnNames() {
    Set<String> attrNameSet = getColumns().keySet();
    return attrNameSet.toArray(new String[attrNameSet.size()]);
  }

  /**
   * Return column values of this record.
   */
  public Object[] getColumnValues() {
    java.util.Collection<Object> attrValueCollection = getColumns().values();
    return attrValueCollection.toArray(new Object[attrValueCollection.size()]);
  }

  @Override
  public Map<String, Object> toMap() {
    return getColumns();
  }

  @Override
  public Row put(Map<String, Object> map) {
    getColumns().putAll(map);
    return this;
  }

  @Override
  public Row put(String key, Object value) {
    getColumns().put(key, value);
    return this;
  }

  @Override
  public int size() {
    return columns != null ? columns.size() : 0;
  }

  /**
   * Converts a Record object to the specified Java bean type.
   * 
   * @param beanClass The type of Java Bean to convert to.
   * @return The converted Java Bean object.
   */
  public <T> T toBean(Class<T> beanClass) {
    return DbKit.getConfig().getRecordConvert().toJavaBean(this, beanClass);
  }

  public Kv toKv() {
    return Kv.create().set(columns);
  }

  /**
   * 将
   * 
   * @param bean
   * @return
   */
  public static Row fromBean(Object bean) {
    return DbKit.getConfig().getRecordConvert().fromJavaBean(bean);
  }

  public static Row by(String column, Object value) {
    Row record = new Row();
    record.getColumns().put(column, value);
    record._getModifyFlag().add(column); // Add modify flag, update() need this flag.
    return record;
  }

  public static Row fromMap(Map<String, Object> recordMap) {
    Row record = new Row();
    return record.setColumns(recordMap);
  }

  public static Row create() {
    return new Row();
  }

}
