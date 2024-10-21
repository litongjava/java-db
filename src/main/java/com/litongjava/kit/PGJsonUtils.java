package com.litongjava.kit;

import java.sql.SQLException;

import org.postgresql.util.PGobject;

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
}
