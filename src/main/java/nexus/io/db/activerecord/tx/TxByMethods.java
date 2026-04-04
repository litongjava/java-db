package nexus.io.db.activerecord.tx;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import com.litongjava.jfinal.aop.AopInterceptor;
import com.litongjava.jfinal.aop.AopInvocation;

import nexus.io.db.activerecord.Config;
import nexus.io.db.activerecord.Db;
import nexus.io.db.activerecord.DbKit;
import nexus.io.model.db.IAtom;

/**
 * TxByMethods
 */
public class TxByMethods implements AopInterceptor {
	
	private Set<String> methodSet = new HashSet<String>();
	
	public TxByMethods(String... methods) {
		if (methods == null || methods.length == 0)
			throw new IllegalArgumentException("methods can not be null.");
		
		for (String method : methods)
			methodSet.add(method.trim());
	}
	
	public void intercept(final AopInvocation inv) {
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







