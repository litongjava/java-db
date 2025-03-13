package com.litongjava.template;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

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

  public static byte[] renderToBytes(File file, Kv kv) {
    Template template = Engine.use().getTemplate(file.getName());
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      template.render(kv, baos);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
