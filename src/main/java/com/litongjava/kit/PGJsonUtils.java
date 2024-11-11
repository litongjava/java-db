package com.litongjava.kit;

import java.sql.SQLException;
import java.util.HashMap;

import org.postgresql.util.PGobject;

import com.jfinal.kit.Kv;
import com.litongjava.db.activerecord.Record;
import com.litongjava.tio.utils.hutool.StrUtil;
import com.litongjava.tio.utils.json.JsonUtils;

public class PGJsonUtils {

  public static PGobject json(Object obj) {
    if (obj == null) {
      return null;
    }
    String json = JsonUtils.toJson(obj);
    return json(json);

  }

  public static PGobject json(String json) {
    PGobject pgObject = new PGobject();
    try {
      pgObject.setType("json");
      pgObject.setValue(json);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return pgObject;
  }

  public static PGobject jsonb(Object obj) {
    if (obj == null) {
      return null;
    }
    String json = JsonUtils.toJson(obj);
    return jsonb(json);
  }

  public static PGobject jsonb(String json) {
    PGobject pgObject = new PGobject();
    try {
      pgObject.setType("jsonb");
      pgObject.setValue(json);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return pgObject;
  }

  public static <T> void toBean(Kv kv, String key, Class<T> clazz) {
    PGobject pgObject1 = kv.getAs(key);
    String value = pgObject1.getValue();
    if (StrUtil.isNotBlank(value)) {
      T setting = JsonUtils.parse(value, clazz);
      kv.set(key, setting);
    } else {
      kv.set(key, new HashMap<>(1));
    }
  }

  public static <T> void toBean(Record record, String key, Class<T> clazz) {
    Object object = record.get(key);
    if (object instanceof PGobject) {
      PGobject pgObject1 = (PGobject) object;
      String value = pgObject1.getValue();
      if (StrUtil.isNotBlank(value)) {
        T setting = JsonUtils.parse(value, clazz);
        record.set(key, setting);
      } else {
        record.set(key, new HashMap<>(1));
      }
    } else {
      return;
    }

  }

}
