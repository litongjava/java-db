package com.litongjava.template;

import com.jfinal.kit.Kv;
import com.jfinal.template.Engine;
import com.jfinal.template.Template;

public class EnjoyTemplate {

  static {
    Engine.use().addDirective("localeDate", LocaleDateDirective.class);
  }

  public static String renderToString(String fileName, Kv by) {
    Template template = Engine.use().getTemplate(fileName);
    String html = template.renderToString(by);
    return html;
  }

  public static String renderToString(String fileName) {
    return Engine.use().getTemplate(fileName).renderToString();
  }
}
