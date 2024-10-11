package com.litongjava.db.activerecord;


import java.sql.Connection;

import com.litongjava.jfinal.aop.AopInterceptor;
import com.litongjava.jfinal.aop.AopInvocation;

import lombok.extern.slf4j.Slf4j;

/**
 * One Connection Per Thread for one request.<br>
 * warning: can not use this interceptor with transaction feature like Tx, Db.tx(...)
 */
@Slf4j
public class OneConnectionPerThread implements AopInterceptor {
	
	public void intercept(AopInvocation inv) {
		Connection conn = DbKit.config.getThreadLocalConnection();
		if (conn != null) {
			inv.invoke();
			return ;
		}
		
		try {
			conn = DbKit.config.getConnection();
			DbKit.config.setThreadLocalConnection(conn);
			inv.invoke();
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		finally {
			DbKit.config.removeThreadLocalConnection();
			if (conn != null) {
				try{conn.close();}catch(Exception e){log.error(e.getMessage(), e);};
			}
		}
	}
}
