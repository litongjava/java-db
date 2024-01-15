package com.litongjava.jfinal.plugin.activerecord.dialect;

public class DialectUtils {
  public static String forDbFindColumnsById(String tableName, String columns, String[] pKeys) {
    StringBuilder sql = new StringBuilder("select ");
    columns = columns.trim();
    if ("*".equals(columns)) {
      sql.append('*');
    }
    else {
      String[] arr = columns.split(",");
      for (int i=0; i<arr.length; i++) {
        if (i > 0) {
          sql.append(',');
        }
        sql.append('`').append(arr[i].trim()).append('`');
      }
    }
    
    sql.append(" from `");
    
    sql.append(tableName);
    sql.append("` where ");
    for (int i=0; i<pKeys.length; i++) {
      if (i > 0) {
        sql.append(" and ");
      }
      sql.append('`').append(pKeys[i]).append("` = ?");
    }
    return sql.toString();
  }

}
