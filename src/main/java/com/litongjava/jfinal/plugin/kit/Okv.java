package com.litongjava.jfinal.plugin.kit;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import com.litongjava.jfinal.plugin.json.Json;

/**
 * Okv (Ordered Key Value)
 *
 * Okv 与 Kv 的唯一区别在于 Okv 继承自 LinkedHashMap，而 Kv 继承自 HashMap
 * 所以对 Okv 中的数据进行迭代输出的次序与数据插入的先后次序一致
 *
 * Example：
 *    Okv para = Okv.by("id", 123);
 *    User user = user.findFirst(getSqlPara("find", para));
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class Okv extends LinkedHashMap {

  private static final long serialVersionUID = -6517132544791494383L;

  public Okv() {
  }

  public static Okv of(Object key, Object value) {
    return new Okv().set(key, value);
  }

  public static Okv by(Object key, Object value) {
    return new Okv().set(key, value);
  }

  public static Okv create() {
    return new Okv();
  }

  public Okv set(Object key, Object value) {
    super.put(key, value);
    return this;
  }

  public Okv setIfNotBlank(Object key, String value) {
    if (StrKit.notBlank(value)) {
      set(key, value);
    }
    return this;
  }

  public Okv setIfNotNull(Object key, Object value) {
    if (value != null) {
      set(key, value);
    }
    return this;
  }

  public Okv set(Map map) {
    super.putAll(map);
    return this;
  }

  public Okv set(Okv okv) {
    super.putAll(okv);
    return this;
  }

  public Okv delete(Object key) {
    super.remove(key);
    return this;
  }

  public <T> T getAs(Object key) {
    return (T) get(key);
  }

  public <T> T getAs(Object key, T defaultValue) {
    Object ret = get(key);
    return ret != null ? (T) ret : defaultValue;
  }

  public String getStr(Object key) {
    Object s = get(key);
    return s != null ? s.toString() : null;
  }

  public Integer getInt(Object key) {
    return TypeKit.toInt(get(key));
  }

  public Long getLong(Object key) {
    return TypeKit.toLong(get(key));
  }

  public BigDecimal getBigDecimal(Object key) {
    return TypeKit.toBigDecimal(get(key));
  }

  public Double getDouble(Object key) {
    return TypeKit.toDouble(get(key));
  }

  public Float getFloat(Object key) {
    return TypeKit.toFloat(get(key));
  }

  public Number getNumber(Object key) {
    return TypeKit.toNumber(get(key));
  }

  public Boolean getBoolean(Object key) {
    return TypeKit.toBoolean(get(key));
  }

  public java.util.Date getDate(Object key) {
    return TypeKit.toDate(get(key));
  }

  public java.time.LocalDateTime getLocalDateTime(Object key) {
    return TypeKit.toLocalDateTime(get(key));
  }

  /**
   * key 存在，并且 value 不为 null
   */
  public boolean notNull(Object key) {
    return get(key) != null;
  }

  /**
   * key 不存在，或者 key 存在但 value 为null
   */
  public boolean isNull(Object key) {
    return get(key) == null;
  }

  /**
   * key 存在，并且 value 为 true，则返回 true
   */
  public boolean isTrue(Object key) {
    Object value = get(key);
    return value != null && TypeKit.toBoolean(value);
  }

  /**
   * key 存在，并且 value 为 false，则返回 true
   */
  public boolean isFalse(Object key) {
    Object value = get(key);
    return value != null && !TypeKit.toBoolean(value);
  }

  public String toJson() {
    return Json.getJson().toJson(this);
  }

  public boolean equals(Object okv) {
    return okv instanceof Okv && super.equals(okv);
  }

  public Okv keep(String... keys) {
    if (keys != null && keys.length > 0) {
      Okv newOkv = Okv.create();
      for (String k : keys) {
        if (containsKey(k)) { // 避免将并不存在的变量存为 null
          newOkv.put(k, get(k));
        }
      }

      clear();
      putAll(newOkv);
    } else {
      clear();
    }

    return this;
  }

  public <K, V> Map<K, V> toMap() {
    return this;
  }
}
