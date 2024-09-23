package com.litongjava.redis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.jfinal.kit.StrKit;

import redis.clients.jedis.Jedis;

/**
 * Redis. redis 工具类
 * 
 * <pre>
 * 例如：
 * Redis.use().set("key", "value");
 * Redis.use().get("key");
 * </pre>
 */
public class Redis {

  static RedisCache mainCache = null;

  private static final ConcurrentHashMap<String, RedisCache> cacheMap = new ConcurrentHashMap<String, RedisCache>(32, 0.5F);

  public static void addCache(RedisCache cache) {
    if (cache == null)
      throw new IllegalArgumentException("cache can not be null");
    if (cacheMap.containsKey(cache.getName()))
      throw new IllegalArgumentException("The cache name already exists");

    cacheMap.put(cache.getName(), cache);
    if (mainCache == null)
      mainCache = cache;
  }

  public static RedisCache removeCache(String cacheName) {
    return cacheMap.remove(cacheName);
  }

  /**
   * 提供一个设置设置主缓存 mainCache 的机会，否则第一个被初始化的 Cache 将成为 mainCache
   */
  public static void setMainCache(String cacheName) {
    if (StrKit.isBlank(cacheName))
      throw new IllegalArgumentException("cacheName can not be blank");
    cacheName = cacheName.trim();
    RedisCache cache = cacheMap.get(cacheName);
    if (cache == null)
      throw new IllegalArgumentException("the cache not exists: " + cacheName);

    Redis.mainCache = cache;
  }

  public static RedisCache use() {
    return mainCache;
  }

  public static RedisCache use(String cacheName) {
    return cacheMap.get(cacheName);
  }

  /**
   * 使用 lambda 开放 Jedis API，建议优先使用本方法
   * 
   * <pre>
   * 例子 1：
   *   Long ret = Redis.call(j -> j.incrBy("key", 1));
   *   
   * 例子 2：
   *   Long ret = Redis.call(jedis -> {
   *       return jedis.incrBy("key", 1);
   *   });
   * </pre>
   */
  public static <R> R call(Function<Jedis, R> jedis) {
    return use().call(jedis);
  }

  /**
   * 使用 lambda 开放 Jedis API，建议优先使用本方法
   * 
   * <pre>
   * 例子：
   *   Long ret = Redis.call("cacheName", j -> j.incrBy("key", 1));
   * </pre>
   */
  public static <R> R call(String cacheName, Function<Jedis, R> jedis) {
    return use(cacheName).call(jedis);
  }

  public static <T> T callback(IRedisCallback<T> callback) {
    return callback(use(), callback);
  }

  public static <T> T callback(String cacheName, IRedisCallback<T> callback) {
    return callback(use(cacheName), callback);
  }

  private static <T> T callback(RedisCache cache, IRedisCallback<T> callback) {
    Jedis jedis = cache.getThreadLocalJedis();
    boolean notThreadLocalJedis = (jedis == null);
    if (notThreadLocalJedis) {
      jedis = cache.jedisPool.getResource();
      cache.setThreadLocalJedis(jedis);
    }
    try {
      return callback.call(cache);
    } finally {
      if (notThreadLocalJedis) {
        cache.removeThreadLocalJedis();
        jedis.close();
      }
    }
  }

  public static <R> R getBean(String key, Class<R> type) {
    return Redis.use().getBean(key, type);
  }

  public static String setBean(String key, long seconds, Object input) {
    return Redis.use().setBean(key, seconds, input);
  }

  public static String setBean(String key, Object input) {
    return Redis.use().setBean(key, input);
  }

  public static String setStr(String key, String input) {
    return Redis.use().setStr(key, input);
  }

  public static <R> String setStr(String key, long seconds, String input) {
    return Redis.use().set(key, seconds, input);
  }

  public static String getStr(String key) {
    return Redis.use().getStr(key);
  }

  public static String setInt(String key, int value) {
    return Redis.use().setInt(key, value);
  }

  public static String setInt(String key, long seconds, int value) {
    return Redis.use().setInt(key, seconds, value);
  }

  public static Integer getInt(String key) {
    return Redis.use().getInt(key);
  }

  public static String setLong(String key, long value) {
    return Redis.use().setLong(key, value);
  }

  public static String setLong(String key, long seconds, long value) {
    return Redis.use().setLong(key, seconds, value);
  }

  public static Long getLong(String key) {
    return Redis.use().getLong(key);
  }
  
  
  public static boolean hasKey(String key) {
    return Redis.use().hasKey(key);
  }
}
