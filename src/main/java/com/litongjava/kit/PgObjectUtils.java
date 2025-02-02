package com.litongjava.kit;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PGobject;

import com.jfinal.kit.Kv;
import com.litongjava.db.activerecord.Row;
import com.litongjava.tio.utils.hutool.StrUtil;
import com.litongjava.tio.utils.json.JsonUtils;

public class PgObjectUtils {

  public static PGobject json(Object obj) {
    if (obj == null) {
      return null;
    }
    String json = JsonUtils.toSkipNullJson(obj);
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
    String json = JsonUtils.toSkipNullJson(obj);
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

  public static <T> void toBean(Row row, String key, Class<T> clazz) {
    Object object = row.get(key);
    if (object instanceof PGobject) {
      PGobject pgObject1 = (PGobject) object;
      String value = pgObject1.getValue();
      if (StrUtil.isNotBlank(value)) {
        T setting = JsonUtils.parse(value, clazz);
        row.set(key, setting);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else if (object instanceof String) {
      String value = (String) object;
      if (StrUtil.isNotBlank(value)) {
        T setting = JsonUtils.parse(value, clazz);
        row.set(key, setting);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else {
      return;
    }
  }

  public static <T> void toListBean(Row row, String key, Class<T> clazz) {
    Object object = row.get(key);
    if (object instanceof PGobject) {
      PGobject pgObject1 = (PGobject) object;
      String value = pgObject1.getValue();
      if (StrUtil.isNotBlank(value)) {
        List<T> setting = JsonUtils.parseArray(value, clazz);
        row.set(key, setting);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else if (object instanceof String) {
      String value = (String) object;
      if (StrUtil.isNotBlank(value)) {
        List<T> setting = JsonUtils.parseArray(value, clazz);
        row.set(key, setting);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else {
      return;
    }
  }

  public static void toMap(Row row, String key) {
    Object object = row.get(key);
    if (object instanceof PGobject) {
      PGobject pgObject1 = (PGobject) object;
      String value = pgObject1.getValue();
      if (StrUtil.isNotBlank(value)) {
        Map<?, ?> map = JsonUtils.parseToMap(value);
        row.set(key, map);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else if (object instanceof String) {
      String value = (String) object;
      if (StrUtil.isNotBlank(value)) {
        Map<?, ?> map = JsonUtils.parseToMap(value);
        row.set(key, map);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else {
      return;
    }
  }

  public static <T> void toListMap(Row row, String key) {
    Object object = row.get(key);
    if (object instanceof PGobject) {
      PGobject pgObject1 = (PGobject) object;
      String value = pgObject1.getValue();
      if (StrUtil.isNotBlank(value)) {
        List<Map<String, Object>> maps = JsonUtils.parseToListMap(value, String.class, Object.class);
        row.set(key, maps);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else if (object instanceof String) {
      String value = (String) object;
      if (StrUtil.isNotBlank(value)) {
        List<Map<String, Object>> maps = JsonUtils.parseToListMap(value, String.class, Object.class);
        row.set(key, maps);
      } else {
        row.set(key, new HashMap<>(1));
      }
    } else {
      return;
    }
  }

  public static <T> T toBean(PGobject pgObject, Class<T> clazz) {
    String value = pgObject.getValue();
    if (StrUtil.isNotBlank(value)) {
      return JsonUtils.parse(value, clazz);
    } else {
      return null;
    }
  }

  public static <T> List<T> toListBean(PGobject pgObject, Class<T> clazz) {
    String value = pgObject.getValue();
    if (StrUtil.isNotBlank(value)) {
      return JsonUtils.parseArray(value, clazz);
    } else {
      return null;
    }
  }

}
