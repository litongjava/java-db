package com.litongjava.jfinal.plugin.activerecord;

/**
 * NestedTransactionHelpException
 * <br>
 * Notice the outer transaction that the nested transaction return false
 */
public class NestedTransactionHelpException extends RuntimeException {
	
	private static final long serialVersionUID = 3813238946083156753L;
	
	public NestedTransactionHelpException(String message) {
		super(message);
	}
	
	/**
	 * 异常构造函数会调用 fillInStackTrace() 构建整个调用栈，消耗较大
	 * 而 NestedTransactionHelpException 无需使用调用栈信息，覆盖
	 * 此方法用于提升性能
	 */
	@Override
	public Throwable fillInStackTrace() {
		return this;
	}
}



