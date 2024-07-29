package com.litongjava.db.activerecord.builder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Model;
import com.litongjava.db.activerecord.ModelBuilder;

/**
 * 针对 mybatis 用户使用习惯，避免 JDBC 将 Byte、Short 转成 Integer
 * 
 * <pre>
 * 使用示例：
 * MySqlDialect dialect = new MySqlDialect();
 * dialect.keepByteAndCharType(true);
 * activeRecordPlugin.setDialect(dialect);
 * </pre>
 */
public class KeepByteAndShortModelBuilder extends ModelBuilder {
	
	public static final KeepByteAndShortModelBuilder me = new KeepByteAndShortModelBuilder();
	
	@Override
	@SuppressWarnings({"rawtypes"})
	public <T> List<T> build(ResultSet rs, Class<? extends Model> modelClass) throws SQLException, ReflectiveOperationException {
		return build(rs, modelClass, null);
	}
	
	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public <T> List<T> build(ResultSet rs, Class<? extends Model> modelClass, Function<T, Boolean> func) throws SQLException, ReflectiveOperationException {
		List<T> result = new ArrayList<T>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		String[] labelNames = new String[columnCount + 1];
		int[] types = new int[columnCount + 1];
		buildLabelNamesAndTypes(rsmd, labelNames, types);
		while (rs.next()) {
			Model<?> ar = modelClass.newInstance();
			Map<String, Object> attrs = CPI.getAttrs(ar);
			for (int i=1; i<=columnCount; i++) {
				Object value;
				int t = types[i];
				if (t < Types.DATE) {
					if (t == Types.TINYINT) {
						value = BuilderKit.getByte(rs, i);
					} else if (t == Types.SMALLINT) {
						value = BuilderKit.getShort(rs, i);
					} else {
						value = rs.getObject(i);
					}
				} else {
					if (t == Types.TIMESTAMP) {
						value = rs.getTimestamp(i);
					} else if (t == Types.DATE) {
						value = rs.getDate(i);
					} else if (t == Types.CLOB) {
						value = handleClob(rs.getClob(i));
					} else if (t == Types.NCLOB) {
						value = handleClob(rs.getNClob(i));
					} else if (t == Types.BLOB) {
						value = handleBlob(rs.getBlob(i));
					} else {
						value = rs.getObject(i);
					}
				}
				
				attrs.put(labelNames[i], value);
			}
			
			if (func == null) {
				result.add((T)ar);
			} else {
				if ( ! func.apply((T)ar) ) {
					break ;
				}
			}
		}
		return result;
	}
}



