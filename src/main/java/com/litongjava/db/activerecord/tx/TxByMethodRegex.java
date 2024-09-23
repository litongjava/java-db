package com.litongjava.db.activerecord.tx;

import java.sql.SQLException;
import java.util.regex.Pattern;

import com.jfinal.kit.StrKit;
import com.litongjava.db.activerecord.Config;
import com.litongjava.db.activerecord.Db;
import com.litongjava.db.activerecord.DbKit;
import com.litongjava.jfinal.aop.Interceptor;
import com.litongjava.jfinal.aop.Invocation;
import com.litongjava.model.db.IAtom;

/**
 * TxByMethodRegex.
 * The regular expression match the method name of the target.
 */
public class TxByMethodRegex implements Interceptor {
	
	private Pattern pattern;
	
	public TxByMethodRegex(String regex) {
		this(regex, true);
	}
	
	public TxByMethodRegex(String regex, boolean caseSensitive) {
		if (StrKit.isBlank(regex))
			throw new IllegalArgumentException("regex can not be blank.");
		
		pattern = caseSensitive ? Pattern.compile(regex) : Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
	}
	
	public void intercept(final Invocation inv) {
		Config config = Tx.getConfigWithTxConfig(inv);
		if (config == null)
			config = DbKit.getConfig();
		
		if (pattern.matcher(inv.getMethodName()).matches()) {
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


