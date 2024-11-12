package com.litongjava.db.activerecord.cache;

import com.litongjava.cache.IDbCache;
import com.litongjava.ehcache.EhCacheKit;

/**
 * EhCache.
 */
public class DefaultEhCache implements IDbCache {

  @SuppressWarnings("unchecked")
  public <T> T get(String cacheName, Object key) {
    return (T) EhCacheKit.get(cacheName, key);
  }

  public void put(String cacheName, Object key, Object value) {
    EhCacheKit.put(cacheName, key, value);
  }

  @Override
  public void put(String cacheName, Object key, Object value, int ttl) {
    EhCacheKit.put(cacheName, key, value, ttl);

  }

  public void remove(String cacheName, Object key) {
    EhCacheKit.remove(cacheName, key);
  }

  public void removeAll(String cacheName) {
    EhCacheKit.removeAll(cacheName);
  }

}
