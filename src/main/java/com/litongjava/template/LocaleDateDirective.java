package com.litongjava.template;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.Date;
import java.util.Locale;

import com.jfinal.template.Directive;
import com.jfinal.template.Env;
import com.jfinal.template.TemplateException;
import com.jfinal.template.expr.ast.Expr;
import com.jfinal.template.expr.ast.ExprList;
import com.jfinal.template.io.Writer;
import com.jfinal.template.stat.ParseException;
import com.jfinal.template.stat.Scope;

/**
 * #localeDate 支持 Locale 参数的日期格式化指令
 *
 * 用法示例：
 * 1：#localeDate(createAt) 使用默认的 datePattern 和系统默认 Locale 格式化日期
 * 2：#localeDate(createAt, "yyyy-MM-dd HH:mm:ss") 指定 datePattern，使用系统默认 Locale 格式化日期
 * 3：#localeDate(createAt, "MMMM dd, yyyy", "en") 指定 datePattern 和 Locale，格式化日期
 * 4：#localeDate() 使用默认的 datePattern 和系统默认 Locale 输出当前日期
 */
public class LocaleDateDirective extends Directive {

  private Expr dateExpr;
  private Expr patternExpr;
  private Expr localeExpr;

  @Override
  public void setExprList(ExprList exprList) {
    int paraNum = exprList.length();
    if (paraNum == 0) {
      this.dateExpr = null;
      this.patternExpr = null;
      this.localeExpr = null;
    } else if (paraNum == 1) {
      this.dateExpr = exprList.getExpr(0);
      this.patternExpr = null;
      this.localeExpr = null;
    } else if (paraNum == 2) {
      this.dateExpr = exprList.getExpr(0);
      this.patternExpr = exprList.getExpr(1);
      this.localeExpr = null;
    } else if (paraNum == 3) {
      this.dateExpr = exprList.getExpr(0);
      this.patternExpr = exprList.getExpr(1);
      this.localeExpr = exprList.getExpr(2);
    } else {
      throw new ParseException("Wrong number parameter of #localeDate directive, three parameters allowed at most", location);
    }
  }

  @Override
  public void exec(Env env, Scope scope, Writer writer) {
    Object date;
    String pattern;
    Locale locale = Locale.getDefault(); // 默认使用系统默认 Locale

    // 处理日期参数
    if (dateExpr != null) {
      date = dateExpr.eval(scope);
    } else {
      date = new Date();
    }

    // 处理格式化模式参数
    if (patternExpr != null) {
      Object temp = patternExpr.eval(scope);
      if (temp instanceof String) {
        pattern = (String) temp;
      } else {
        throw new TemplateException("The second parameter datePattern of #localeDate directive must be String", location);
      }
    } else {
      pattern = env.getEngineConfig().getDatePattern();
    }

    // 处理 Locale 参数
    if (localeExpr != null) {
      Object localeVal = localeExpr.eval(scope);
      if (localeVal instanceof Locale) {
        locale = (Locale) localeVal;
      } else if (localeVal instanceof String) {
        // 简单处理：假设传入的字符串为语言代码，如 "en"、"zh" 等
        locale = new Locale((String) localeVal);
      } else {
        throw new TemplateException("The third parameter locale of #localeDate directive must be Locale or String", location);
      }
    }

    write(date, pattern, locale, writer);
  }

  private void write(Object date, String pattern, Locale locale, Writer writer) {
    try {
      if (date instanceof Date) {
        // 使用 SimpleDateFormat 处理 java.util.Date
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, locale);
        writer.write(sdf.format((Date) date));
      } else if (date instanceof Temporal) {
        // 使用 DateTimeFormatter 处理 Java 8+ 的 Temporal 类型
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, locale);
        writer.write(formatter.format((Temporal) date));
      } else if (date != null) {
        throw new TemplateException("The first parameter of #localeDate directive cannot be " + date.getClass().getName(), location);
      }
    } catch (TemplateException | ParseException e) {
      throw e;
    } catch (Exception e) {
      throw new TemplateException(e.getMessage(), location, e);
    }
  }
}
