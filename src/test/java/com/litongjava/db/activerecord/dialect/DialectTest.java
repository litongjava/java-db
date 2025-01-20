package com.litongjava.db.activerecord.dialect;

import org.junit.Test;

import com.litongjava.db.kit.SqlParseKit;

public class DialectTest {

  @Test
  public void test() {
    String sql = "FROM convoy WHERE chat_type = 'channel' ORDER BY SUBSTRING(name, 1, 1) ASC, CAST(SUBSTRING(name, 2) AS UNSIGNED) ASC";
    String replaceOrderBy = SqlParseKit.replaceOrderBy(sql);
    System.out.println(replaceOrderBy);

    sql = "FROM convoy WHERE chat_type = 'channel' ORDER BY id asc";
    replaceOrderBy = SqlParseKit.replaceOrderBy(sql);
    System.out.println(replaceOrderBy);

    sql = "FROM convoy WHERE chat_type = 'channel' ORDER BY id asc,create_time desc";
    replaceOrderBy = SqlParseKit.replaceOrderBy(sql);
    System.out.println(replaceOrderBy);

  }

}
