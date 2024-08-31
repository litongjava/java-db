package com.litongjava.redis.serializer;

/**
 * JdkSerializer.
 */
public class KeyValueJdkSerializer implements ISerializer {

  public static final ISerializer me = new KeyValueJdkSerializer();

  public byte[] keyToBytes(String key) {
    return valueToBytes(key);
  }

  public String keyFromBytes(byte[] bytes) {
    return (String) valueFromBytes(bytes);
  }

  public byte[] fieldToBytes(Object field) {
    return valueToBytes(field);
  }

  public Object fieldFromBytes(byte[] bytes) {
    return valueFromBytes(bytes);
  }

  public byte[] valueToBytes(Object value) {
    return JdkSerializer.me.valueToBytes(value);
  }

  public Object valueFromBytes(byte[] bytes) {
    return JdkSerializer.me.valueFromBytes(bytes);
  }
}
