package com.litongjava.db.activerecord.stat;

import java.util.List;

public interface ISqlStatementStat {
  public void save(String name, String sqlType, String sql, Object[] paras, int size, long start, long elapsed, boolean writeSync);
  
  public void save(String name, String sqlType, String sql, @SuppressWarnings("rawtypes") List paras, int size, long start, long elapsed, boolean writeSync);
}