package com.litongjava.ehcache;

import com.litongjava.cache.Cacheable;
import com.litongjava.jfinal.aop.Interceptor;
import com.litongjava.jfinal.aop.Invocation;

/**
 * EvictInterceptor.
 */
public class EvictInterceptor implements Interceptor {

  public void intercept(Invocation inv) {
    inv.invoke();

    // @CacheName 注解中的多个 cacheName 可用逗号分隔
    String[] cacheNames = getCacheName(inv).split(",");
    if (cacheNames.length == 1) {
      EhCache.removeAll(cacheNames[0].trim());
    } else {
      for (String cn : cacheNames) {
        EhCache.removeAll(cn.trim());
      }
    }
  }

  /**
   * 获取 @CacheName 注解配置的 cacheName，注解可配置在方法和类之上
   */
  protected String getCacheName(Invocation inv) {
    Cacheable cacheName = inv.getMethod().getAnnotation(Cacheable.class);
    if (cacheName != null) {
      return cacheName.value();
    }

    cacheName = inv.getTarget().getClass().getAnnotation(Cacheable.class);
    if (cacheName == null) {
      throw new RuntimeException("EvictInterceptor need CacheName annotation in controller.");
    }

    return cacheName.value();
  }
}
