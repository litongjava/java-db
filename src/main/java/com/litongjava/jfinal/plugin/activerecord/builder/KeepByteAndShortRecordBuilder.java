package com.litongjava.jfinal.plugin.activerecord.builder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.litongjava.jfinal.plugin.activerecord.CPI;
import com.litongjava.jfinal.plugin.activerecord.Config;
import com.litongjava.jfinal.plugin.activerecord.ModelBuilder;
import com.litongjava.jfinal.plugin.activerecord.Record;
import com.litongjava.jfinal.plugin.activerecord.RecordBuilder;

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
public class KeepByteAndShortRecordBuilder extends RecordBuilder {
	
	public static final KeepByteAndShortRecordBuilder me = new KeepByteAndShortRecordBuilder();
	
	@Override
	public List<Record> build(Config config, ResultSet rs) throws SQLException {
		return build(config, rs, null);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public List<Record> build(Config config, ResultSet rs, Function<Record, Boolean> func) throws SQLException {
		List<Record> result = new ArrayList<Record>();
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		String[] labelNames = new String[columnCount + 1];
		int[] types = new int[columnCount + 1];
		buildLabelNamesAndTypes(rsmd, labelNames, types);
		while (rs.next()) {
			Record record = new Record();
			CPI.setColumnsMap(record, config.getContainerFactory().getColumnsMap());
			Map<String, Object> columns = record.getColumns();
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
						value = ModelBuilder.me.handleClob(rs.getClob(i));
					} else if (t == Types.NCLOB) {
						value = ModelBuilder.me.handleClob(rs.getNClob(i));
					} else if (t == Types.BLOB) {
						value = ModelBuilder.me.handleBlob(rs.getBlob(i));
					} else {
						value = rs.getObject(i);
					}
				}
				
				columns.put(labelNames[i], value);
			}
			
			if (func == null) {
				result.add(record);
			} else {
				if ( ! func.apply(record) ) {
					break ;
				}
			}
		}
		return result;
	}
}



