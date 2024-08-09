package com.litongjava.es.client;

import java.net.URI;

public class EsDSNUtils {

  public static EsDSN parse(String dsn) {
    EsDSN esInfo = new EsDSN();
    // 解析DSN
    URI uri = URI.create(dsn);
    String userInfo = uri.getUserInfo();
    if (userInfo != null && !userInfo.isEmpty()) {
      String[] userInfoParts = userInfo.split(":");
      String username = userInfoParts[0];
      String password = userInfoParts[1];
      esInfo.setUsername(username);
      esInfo.setPassword(password);
    }

    String host = uri.getHost();
    int port = uri.getPort();
    String scheme = uri.getScheme();
    esInfo.setHost(host);
    esInfo.setPort(port);
    esInfo.setSchema(scheme);
    esInfo.setUrl(scheme + "://" + host + ":" + port);

    return esInfo;
  }
}
