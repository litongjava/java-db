package com.litongjava.db.activerecord;

/**
 * ActiveRecordException
 */
public class ActiveRecordException extends RuntimeException {

  private static final long serialVersionUID = 342820722361408621L;

  private String sql;
  private Object[] paras;

  public ActiveRecordException(String message) {
    super(message);
  }

  public ActiveRecordException(Throwable cause) {
    super(cause);
  }

  public ActiveRecordException(String message, Throwable cause) {
    super(message, cause);
  }

  public ActiveRecordException(String message, String sql, Throwable cause) {
    super(message, cause);
    this.sql = sql;
  }

  public ActiveRecordException(String message, String sql, Object[] paras, Throwable cause) {
    super(message, cause);
    this.sql = sql;
    this.paras = paras;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public Object[] getParas() {
    return paras;
  }

  public void setParas(Object[] paras) {
    this.paras = paras;
  }
}
