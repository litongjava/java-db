package com.litongjava.template;

import com.jfinal.kit.Kv;
import com.jfinal.template.Engine;
import com.litongjava.tio.utils.environment.EnvUtils;

public class PromptEngine {

  public static final String RESOURCE_BASE_PATH = "/prompts/";
  public static Engine engine;
  static {
    engine = Engine.create("prompt");
    engine.setBaseTemplatePath(RESOURCE_BASE_PATH);
    engine.setToClassPathSourceFactory();
    if (EnvUtils.isDev()) {
      // 支持模板热加载，绝大多数生产环境下也建议配置成 true，除非是极端高性能的场景
      engine.setDevMode(true);
    }

    // 配置极速模式，性能提升 13%
    Engine.setFastMode(true);
    // jfinal 4.9.02 新增配置：支持中文表达式、中文变量名、中文方法名、中文模板函数名
    Engine.setChineseExpression(true);

  }
  public static String renderToString(String fileName, Kv kv) {
    return engine.getTemplate(fileName).renderToString(kv);
  }
}
