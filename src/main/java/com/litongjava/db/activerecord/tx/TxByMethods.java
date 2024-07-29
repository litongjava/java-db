package com.litongjava.db.activerecord.tx;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.litongjava.db.activerecord.Config;
import com.litongjava.db.activerecord.Db;
import com.litongjava.db.activerecord.DbKit;
import com.litongjava.db.activerecord.IAtom;
import com.litongjava.jfinal.aop.Interceptor;
import com.litongjava.jfinal.aop.Invocation;

/**
 * TxByMethods
 */
public class TxByMethods implements Interceptor {
	
	private Set<String> methodSet = new HashSet<String>();
	
	public TxByMethods(String... methods) {
		if (methods == null || methods.length == 0)
			throw new IllegalArgumentException("methods can not be null.");
		
		for (String method : methods)
			methodSet.add(method.trim());
	}
	
	public void intercept(final Invocation inv) {
		Config config = Tx.getConfigWithTxConfig(inv);
		if (config == null)
			config = DbKit.getConfig();
		
		if (methodSet.contains(inv.getMethodName())) {
			Db.use(config.getName()).tx(new IAtom() {
				public boolean run() throws SQLException {
					inv.invoke();
					return true;
				}});
		}
		else {
			inv.invoke();
		}
	}
}







