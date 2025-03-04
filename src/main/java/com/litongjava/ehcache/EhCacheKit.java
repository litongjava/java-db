package com.litongjava.ehcache;

import java.io.Serializable;
import java.util.List;

import com.jfinal.kit.Kv;

import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * CacheKit. Useful tool box for EhCache.
 */
@Slf4j
public class EhCacheKit {

  private static CacheManager cacheManager;

  static void init(CacheManager cacheManager) {
    EhCacheKit.cacheManager = cacheManager;
  }

  public static CacheManager getCacheManager() {
    return cacheManager;
  }

  static Cache getOrAddCache(String cacheName) {
    Cache cache = cacheManager.getCache(cacheName);
    if (cache == null) {
      synchronized (EhCacheKit.class) {
        cache = cacheManager.getCache(cacheName);
        if (cache == null) {
          log.warn("Could not find cache config [" + cacheName + "], using default.");
          cacheManager.addCacheIfAbsent(cacheName);
          cache = cacheManager.getCache(cacheName);
          log.debug("Cache [" + cacheName + "] started.");
        }
      }
    }
    return cache;
  }

  public static void put(String cacheName, Object key, Object value) {
    getOrAddCache(cacheName).put(new Element(key, value));
  }

  public static void put(String cacheName, Object key, Object value, int ttl) {
    Cache cache = getOrAddCache(cacheName);
    Element element = new Element(key, value);
    element.setTimeToLive(ttl);
    cache.put(element);
  }

  @SuppressWarnings("unchecked")
  public static <T> T get(String cacheName, Object key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (T) element.getObjectValue() : null;
  }

  @SuppressWarnings("unchecked")
  public static <T> T get(String cacheName, Serializable key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (T) element.getObjectValue() : null;
  }

  @SuppressWarnings("unchecked")
  public static <T> T get(String cacheName, String key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (T) element.getObjectValue() : null;
  }

  @SuppressWarnings("unchecked")
  public static <T> T get(String cacheName, Long key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (T) element.getObjectValue() : null;
  }

  @SuppressWarnings("unchecked")
  public static <T> T get(String cacheName, Integer key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (T) element.getObjectValue() : null;
  }

  //Boolean 类型
  public static Boolean getBoolean(String cacheName, String key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (boolean) element.getObjectValue() : null;
  }

  public static Boolean getBoolean(String cacheName, Long key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (boolean) element.getObjectValue() : null;
  }

  public static Boolean getBoolean(String cacheName, Integer key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (boolean) element.getObjectValue() : null;
  }

  public static Boolean getBoolean(String cacheName, Serializable key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (boolean) element.getObjectValue() : null;
  }

  public static Boolean getBoolean(String cacheName, Object key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (boolean) element.getObjectValue() : null;
  }

  //String 类型
  public static String getString(String cacheName, String key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (String) element.getObjectValue() : null;
  }

  public static String getString(String cacheName, Long key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (String) element.getObjectValue() : null;
  }

  public static String getString(String cacheName, Integer key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (String) element.getObjectValue() : null;
  }

  public static String getString(String cacheName, Object key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (String) element.getObjectValue() : null;
  }

  public static String getString(String cacheName, Serializable key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (String) element.getObjectValue() : null;
  }

  //Long 类型
  public static Long getLong(String cacheName, String key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Long) element.getObjectValue() : null;
  }

  public static Long getLong(String cacheName, Long key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Long) element.getObjectValue() : null;
  }

  public static Long getLong(String cacheName, Integer key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Long) element.getObjectValue() : null;
  }

  public static Long getLong(String cacheName, Object key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Long) element.getObjectValue() : null;
  }

  public static Long getLong(String cacheName, Serializable key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Long) element.getObjectValue() : null;
  }

  //Integer 类型
  public static Integer getInteger(String cacheName, String key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Integer) element.getObjectValue() : null;
  }

  public static Integer getInteger(String cacheName, Long key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Integer) element.getObjectValue() : null;
  }

  public static Integer getInteger(String cacheName, Integer key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Integer) element.getObjectValue() : null;
  }

  public static Integer getInteger(String cacheName, Object key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Integer) element.getObjectValue() : null;
  }

  public static Integer getInteger(String cacheName, Serializable key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (Integer) element.getObjectValue() : null;
  }

  @SuppressWarnings("unchecked")
  public static List<Kv> getListKv(String cacheName, Object key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (List<Kv>) element.getObjectValue() : null;
  }

  @SuppressWarnings("unchecked")
  public static List<Kv> getListKv(String cacheName, Serializable key) {
    Element element = getOrAddCache(cacheName).get(key);
    return element != null ? (List<Kv>) element.getObjectValue() : null;
  }

  @SuppressWarnings("rawtypes")
  public static List getKeys(String cacheName) {
    return getOrAddCache(cacheName).getKeys();
  }

  public static void remove(String cacheName, Object key) {
    getOrAddCache(cacheName).remove(key);
  }

  public static void removeAll(String cacheName) {
    getOrAddCache(cacheName).removeAll();
  }

  @SuppressWarnings("unchecked")
  public static <T> T get(String cacheName, Object key, IDataLoader dataLoader) {
    Object data = get(cacheName, key);
    if (data == null) {
      data = dataLoader.load();
      put(cacheName, key, data);
    }
    return (T) data;
  }

  @SuppressWarnings("unchecked")
  public static <T> T get(String cacheName, Object key, Class<? extends IDataLoader> dataLoaderClass) {
    Object data = get(cacheName, key);
    if (data == null) {
      try {
        IDataLoader dataLoader = dataLoaderClass.newInstance();
        data = dataLoader.load();
        put(cacheName, key, data);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return (T) data;
  }

}
