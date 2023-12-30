package com.litongjava.jfinal.plugin.activerecord.builder;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * JDBC 获取 Byte 和 Short 时，把 null 转换成了 0，很多时候 0 是有意义的，容易引发业务错误
 * 
 * @author tanyaowu
 */
public class BuilderKit {
	
	public static Byte getByte(ResultSet rs, int i) throws SQLException {
		Object value = rs.getObject(i);
		if (value != null) {
			value = Byte.parseByte(value + "");
			return (Byte)value;
		} else {
			return null;
		}
	}
	
	public static Short getShort(ResultSet rs, int i) throws SQLException {
		Object value = rs.getObject(i);
		if (value != null) {
			value = Short.parseShort(value + "");
			return (Short)value;
		} else {
			return null;
		}
	}
}

