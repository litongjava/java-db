package com.litongjava.jfinal.plugin.activerecord;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ICallback provide a JDBC Connection if you need it or the active record plugin can not satisfy you requirement.
 */
@FunctionalInterface
public interface ICallback {
	
	/**
	 * Place codes here that need call back by active record plugin.
	 * @param conn the JDBC Connection, you need't close this connection after used it, active record plugin will close it automatically
	 */
	Object call(Connection conn) throws SQLException;
}
