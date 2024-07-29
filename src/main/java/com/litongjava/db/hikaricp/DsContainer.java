package com.litongjava.db.hikaricp;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

public class DsContainer {

  public static DataSource ds;
  public static Map<String, DataSource> dsMap = new HashMap<>();

  public static void setDataSource(DataSource ds) {
    DsContainer.ds = ds;
  }

  public static void add(String name, DataSource ds) {
    dsMap.put(name, ds);
  }

  public static DataSource get(String name) {
    return dsMap.get(name);
  }

}
