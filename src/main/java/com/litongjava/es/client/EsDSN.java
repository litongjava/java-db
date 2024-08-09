package com.litongjava.es.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EsDSN {
  private String host;
  private int port;
  private String schema;
  private String username, password;
  private String url;
}
