package com.litongjava.jfinal.plugin.ehcache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.litongjava.jfinal.aop.Interceptor;
import com.litongjava.jfinal.aop.Invocation;
import com.litongjava.jfinal.plugin.cache.CacheableModel;

/**
 * CacheInterceptor.
 */
public class EcacheCacheInterceptor implements Interceptor {

  private static ConcurrentHashMap<String, ReentrantLock> lockMap = new ConcurrentHashMap<String, ReentrantLock>(512);

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
    Object target = inv.getTarget();
    CacheableModel cacheableModel = CacheableModel.buildCacheModel(inv, target);
    String cacheName = cacheableModel.getName();
    String cacheKey = cacheableModel.getKey();
    Object cacheData = CacheKit.get(cacheName, cacheKey);
    if (cacheData == null) {
      Lock lock = getLock(cacheName);
      lock.lock(); // prevent cache snowslide
      try {
        cacheData = CacheKit.get(cacheName, cacheKey);
        if (cacheData == null) {
          Object returnValue = inv.invoke();
          cacheMethodReturnValue(cacheName, cacheKey, returnValue);
          return;
        }
      } finally {
        lock.unlock();
      }
    }

    // useCacheDataAndReturn(cacheData, target);
    inv.setReturnValue(cacheData);
  }

  protected void cacheMethodReturnValue(String cacheName, String cacheKey, Object returnValue) {
    CacheKit.put(cacheName, cacheKey, returnValue);
  }
}
