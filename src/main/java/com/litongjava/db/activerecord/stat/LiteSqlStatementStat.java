package com.litongjava.db.activerecord.stat;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.litongjava.lite.Lite;

/**
 * 保存数据到sql lite
 * 
 * @author Tong Li
 *
 */
public class LiteSqlStatementStat implements ISqlStatementStat {

  @Override
  public void save(String name, String sqlType, String sql, Object[] paras, int size, long startTimeMillis, long elapsed, boolean writeSync) {
    if (writeSync) {
      Lite.saveSqlStatementStat(name, sqlType, sql, paras, size, startTimeMillis, elapsed);
    } else {
      CompletableFuture.runAsync(() -> {
        Lite.saveSqlStatementStat(name, sqlType, sql, paras, size, startTimeMillis, elapsed);
      });
    }
  }

  @Override
  public void save(String name, String sqlType, String sql, @SuppressWarnings("rawtypes")List paras, int size, long startTimeMillis, long elapsed, boolean writeSync) {
    if (writeSync) {
      Lite.saveSqlStatementStat(name, sqlType, sql, paras, size, startTimeMillis, elapsed);
    } else {
      CompletableFuture.runAsync(() -> {
        Lite.saveSqlStatementStat(name, sqlType, sql, paras, size, startTimeMillis, elapsed);
      });
    }

  }

}
