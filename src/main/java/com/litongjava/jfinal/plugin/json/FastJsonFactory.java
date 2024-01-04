package com.litongjava.jfinal.plugin.json;

/**
 * IJsonFactory 的 fastjson 实现.
 */
public class FastJsonFactory implements IJsonFactory {

  private static final FastJsonFactory me = new FastJsonFactory();

  public FastJsonFactory() {
    // 尽早触发 fastjson 的配置代码
    new FastJson();
  }

  public static FastJsonFactory me() {
    return me;
  }

  public Json getJson() {
    return new FastJson();
  }

}
