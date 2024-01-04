package com.litongjava.jfinal.plugin.json;

/**
 * IJsonFactory 的 jfinal 实现.
 */
public class JFinalJsonFactory implements IJsonFactory {

  private static final JFinalJsonFactory me = new JFinalJsonFactory();

  public static JFinalJsonFactory me() {
    return me;
  }

  public Json getJson() {
    return new JFinalJson();
  }
}
