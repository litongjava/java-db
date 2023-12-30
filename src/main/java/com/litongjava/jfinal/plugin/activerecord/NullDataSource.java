package com.litongjava.jfinal.plugin.activerecord;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * NullDataSource.
 */
public class NullDataSource implements DataSource {

	private final String msg = "Can not invoke the method of NullDataSource";

	public PrintWriter getLogWriter() throws SQLException {
		throw new RuntimeException(msg);
	}

	public void setLogWriter(PrintWriter out) throws SQLException {
		throw new RuntimeException(msg);
	}

	public void setLoginTimeout(int seconds) throws SQLException {
		throw new RuntimeException(msg);
	}

	public int getLoginTimeout() throws SQLException {
		throw new RuntimeException(msg);
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new RuntimeException(msg);
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new RuntimeException(msg);
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new RuntimeException(msg);
	}

	public Connection getConnection() throws SQLException {
		throw new RuntimeException(msg);
	}

	public Connection getConnection(String username, String password) throws SQLException {
		throw new RuntimeException(msg);
	}
}




