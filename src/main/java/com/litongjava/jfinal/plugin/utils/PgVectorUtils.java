package com.litongjava.jfinal.plugin.utils;

import java.sql.SQLException;

import org.postgresql.util.PGobject;

public class PgVectorUtils {

  public static PGobject getPgVector(String vectorString) {
    // 使用PGobject来设置vector类型
    PGobject nameVector = new PGobject();
    nameVector.setType("vector");
    try {
      nameVector.setValue(vectorString);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return nameVector;
  }
}
