package com.litongjava.jfinal.plugin.hikaricp;

import javax.sql.DataSource;

public class DsContainer {

  public static DataSource ds;

  public static void setDataSource(DataSource ds) {
    DsContainer.ds = ds;
  }

}
