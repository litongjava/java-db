package com.litongjava.db.activerecord.cache;

import com.litongjava.cache.ICache;
import com.litongjava.ehcache.EhCache;

/**
 * EhCache.
 */
public class DefaultEhCache implements ICache {

  @SuppressWarnings("unchecked")
  public <T> T get(String cacheName, Object key) {
    return (T) EhCache.get(cacheName, key);
  }

  public void put(String cacheName, Object key, Object value) {
    EhCache.put(cacheName, key, value);
  }

  @Override
  public void put(String cacheName, Object key, Object value, int ttl) {
    EhCache.put(cacheName, key, value, ttl);

  }

  public void remove(String cacheName, Object key) {
    EhCache.remove(cacheName, key);
  }

  public void removeAll(String cacheName) {
    EhCache.removeAll(cacheName);
  }

}
