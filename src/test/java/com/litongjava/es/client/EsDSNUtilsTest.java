package com.litongjava.es.client;

import org.junit.Test;

import com.litongjava.tio.utils.json.JsonUtils;

public class EsDSNUtilsTest {

  @Test
  public void test() {
    String dsn = "http://elastic:YourElasticPassword@192.168.1.2:9200";
    EsDSN esInfo = EsDSNUtils.parse(dsn);
    System.out.println(JsonUtils.toJson(esInfo));
  }

}
