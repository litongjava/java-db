package com.litongjava.ehcache;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.litongjava.cache.CacheableModel;
import com.litongjava.jfinal.aop.AopInterceptor;
import com.litongjava.jfinal.aop.AopInvocation;

/**
 * CacheInterceptor.
 */
public class EhCacheInterceptor implements AopInterceptor {

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

  final public void intercept(AopInvocation inv) {
    Object target = inv.getTarget();
    Method method = inv.getMethod();
    Object[] args = inv.getArgs();
    CacheableModel cacheableModel = CacheableModel.buildCacheModel(target, method, args);
    String cacheName = cacheableModel.getName();
    String cacheKey = cacheableModel.getKey();
    Object cacheData = EhCacheKit.get(cacheName, cacheKey);
    if (cacheData == null) {
      Lock lock = getLock(cacheName);
      lock.lock(); // prevent cache snowslide
      try {
        cacheData = EhCacheKit.get(cacheName, cacheKey);
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
    EhCacheKit.put(cacheName, cacheKey, returnValue);
  }
}
