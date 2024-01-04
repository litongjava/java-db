package com.litongjava.jfinal.plugin.json;

/**
 * JFinalJson 与 FastJson 混合做 json 转换
 * toJson 用 JFinalJson，parse 用 FastJson
 * 
 * 注意：
 * 1：需要添加 fastjson 相关 jar 包
 * 2：parse 方法转对象依赖于 setter 方法
 */
public class MixedJson extends Json {

  private JFinalJson jFinalJson;
  private FastJson fastJson;

  public static MixedJson getJson() {
    return new MixedJson();
  }

  public String toJson(Object object) {
    return getJFinalJson().toJson(object);
  }

  public <T> T parse(String jsonString, Class<T> type) {
    return getFastJson().parse(jsonString, type);
  }

  private JFinalJson getJFinalJson() {
    if (jFinalJson == null) {
      jFinalJson = JFinalJson.getJson();
    }
    if (datePattern != null) {
      jFinalJson.setDatePattern(datePattern);
    }
    return jFinalJson;
  }

  private FastJson getFastJson() {
    if (fastJson == null) {
      fastJson = FastJson.getJson();
    }
    if (datePattern != null) {
      fastJson.setDatePattern(datePattern);
    }
    return fastJson;
  }
}
