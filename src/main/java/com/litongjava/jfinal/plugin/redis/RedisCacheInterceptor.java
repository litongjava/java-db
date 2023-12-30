package com.litongjava.jfinal.plugin.redis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.litongjava.jfinal.aop.Interceptor;
import com.litongjava.jfinal.aop.Invocation;
import com.litongjava.jfinal.plugin.cache.CacheableModel;

import redis.clients.jedis.Jedis;

/**
 * CacheInterceptor.
 */
public class RedisCacheInterceptor implements Interceptor {

  private static ConcurrentHashMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<String, ReentrantLock>(512);

  protected Cache getCache() {
    return Redis.use();
  }

  private ReentrantLock getLock(String key) {
    ReentrantLock lock = lockMap.get(key);
    if (lock != null) {
      return lock;
    }

    lock = new ReentrantLock();
    ReentrantLock previousLock = lockMap.putIfAbsent(key, lock);
    return previousLock == null ? lock : previousLock;
  }

  final public void intercept(Invocation inv) {
    Cache cache = getCache();
    Jedis jedis = cache.getThreadLocalJedis();

    if (jedis != null) {
      putIfNotExists(inv, cache, jedis);
    }

    try {
      jedis = cache.jedisPool.getResource();
      cache.setThreadLocalJedis(jedis);
      putIfNotExists(inv, cache, jedis);
    } finally {
      cache.removeThreadLocalJedis();
      jedis.close();
    }

  }

  private void putIfNotExists(Invocation inv, Cache cache, Jedis jedis) {
    Object target = inv.getTarget();
    CacheableModel cacheableModel = CacheableModel.buildCacheModel(inv, target);
    String redisKey = cacheableModel.getName() + "_" + cacheableModel.getKey();
    String cacheData = cache.get(redisKey);

    if (cacheData == null) {
      Lock lock = getLock(redisKey);
      lock.lock(); // prevent cache snowslide
      try {
        Object returnValue = inv.invoke();
        cacheData = cache.get(redisKey);
        if (cacheData == null) {
          cache.setex(redisKey, cacheableModel.getTtl(), returnValue);
          return;
        }
      } finally {
        lock.unlock();
      }
    }
    // useCacheDataAndReturn(cacheData, target);
    inv.setReturnValue(cacheData);
  }
}
