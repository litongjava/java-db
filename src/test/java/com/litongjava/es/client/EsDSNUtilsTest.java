package com.litongjava.es.client;

import org.junit.Test;

import nexus.io.es.client.EsDSN;
import nexus.io.es.client.EsDSNUtils;
import nexus.io.tio.utils.json.JsonUtils;

public class EsDSNUtilsTest {

  @Test
  public void test() {
    String dsn = "http://elastic:YourElasticPassword@192.168.1.2:9200";
    EsDSN esInfo = EsDSNUtils.parse(dsn);
    System.out.println(JsonUtils.toJson(esInfo));
  }

}
