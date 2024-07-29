package com.litongjava.db.activerecord.tx;

import java.sql.Connection;
import java.sql.SQLException;

import com.litongjava.db.activerecord.ActiveRecordException;
import com.litongjava.db.activerecord.Config;
import com.litongjava.db.activerecord.DbKit;
import com.litongjava.db.activerecord.NestedTransactionHelpException;
import com.litongjava.jfinal.aop.Interceptor;
import com.litongjava.jfinal.aop.Invocation;

import lombok.extern.slf4j.Slf4j;

/**
 * ActiveRecord declare transaction.
 * Example: @Before(Tx.class)
 */
@Slf4j
public class Tx implements Interceptor {

	private static TxFun txFun = null;

	public static void setTxFun(TxFun txFun) {
		if (Tx.txFun != null) {
			log.warn("txFun already set");
		}
		Tx.txFun = txFun;
	}

	public static TxFun getTxFun() {
		return Tx.txFun;
	}

	public static Config getConfigWithTxConfig(Invocation inv) {
		TxConfig txConfig = inv.getMethod().getAnnotation(TxConfig.class);
		if (txConfig == null)
			txConfig = inv.getTarget().getClass().getAnnotation(TxConfig.class);

		if (txConfig != null) {
			Config config = DbKit.getConfig(txConfig.value());
			if (config == null)
				throw new RuntimeException("Config not found with TxConfig: " + txConfig.value());
			return config;
		}
		return null;
	}

	protected int getTransactionLevel(Config config) {
		return config.getTransactionLevel();
	}

	public void intercept(Invocation inv) {
		Config config = getConfigWithTxConfig(inv);
		if (config == null)
			config = DbKit.getConfig();

		Connection conn = config.getThreadLocalConnection();
		if (conn != null) {	// Nested transaction support
			try {
				if (conn.getTransactionIsolation() < getTransactionLevel(config))
					conn.setTransactionIsolation(getTransactionLevel(config));
				
				if (txFun == null) {
				    inv.invoke();
				} else {
				    txFun.call(inv, conn);
				}
				
				return ;
			} catch (SQLException e) {
				throw new ActiveRecordException(e);
			}
		}

		Boolean autoCommit = null;
		try {
			conn = config.getConnection();
			autoCommit = conn.getAutoCommit();
			config.setThreadLocalConnection(conn);
			conn.setTransactionIsolation(getTransactionLevel(config));	// conn.setTransactionIsolation(transactionLevel);
			conn.setAutoCommit(false);

			if (txFun == null) {
				inv.invoke();
				conn.commit();
			} else {
				txFun.call(inv, conn);
			}

		} catch (NestedTransactionHelpException e) {
			if (conn != null) try {conn.rollback();} catch (Exception e1) {log.error(e1.getMessage(), e1);}
			//LogKit.logNothing(e);
		} catch (Throwable t) {
			if (conn != null) try {conn.rollback();} catch (Exception e1) {log.error(e1.getMessage(), e1);}

			// 支持在 controller 中 try catch 的 catch 块中使用 render(...) 并 throw e，实现灵活控制 render
			if (txFun == null) {
				log.error(t.getMessage(), t);
			} else {
				throw t instanceof RuntimeException ? (RuntimeException)t : new ActiveRecordException(t);
			}
		}
		finally {
			try {
				if (conn != null) {
					if (autoCommit != null)
						conn.setAutoCommit(autoCommit);
					conn.close();
				}
			} catch (Throwable t) {
				log.error(t.getMessage(), t);	// can not throw exception here, otherwise the more important exception in previous catch block can not be thrown
			}
			finally {
				config.removeThreadLocalConnection();	// prevent memory leak
			}
		}
	}
}



