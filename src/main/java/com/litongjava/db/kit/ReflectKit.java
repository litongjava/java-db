package com.litongjava.db.kit;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

/**
 * 反射工具类
 */
public class ReflectKit {

  public static Object newInstance(Class<?> clazz) {
    try {
      return clazz.newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getMethodSignature(Method method) {
    StringBuilder ret = new StringBuilder().append(method.getDeclaringClass().getName()).append(".")
        .append(method.getName()).append("(");

    int index = 0;
    Parameter[] paras = method.getParameters();
    for (Parameter p : paras) {
      if (index++ > 0) {
        ret.append(", ");
      }
      ret.append(p.getParameterizedType().getTypeName());
    }

    return ret.append(")").toString();
  }

  /*
   * public static String getMethodSignature(Method method) { StringBuilder ret = new StringBuilder() .append(method.getDeclaringClass().getName()) .append(".") .append(method.getName()) .append("(");
   * 
   * int index = 0; java.lang.reflect.Type[] paraTypes = method.getGenericParameterTypes(); for (java.lang.reflect.Type type : paraTypes) { if (index++ > 0) { ret.append(", "); } ret.append(type.getTypeName()); }
   * 
   * return ret.append(")").toString(); }
   */

}
