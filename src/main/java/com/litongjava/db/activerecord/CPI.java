package com.litongjava.db.activerecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.litongjava.model.page.Page;

/**
 * Cross Package Invoking pattern for package activerecord.
 *
 * 为了避免开发者误用，Model、Db 中的部分方法没有完全开放出来，不能直接调用，
 * 但可以通过 CPI 访问那些未完全开放的方法，对于扩展性开发十分有用
 *
 * 例如：
 *     Map attrMap = CPI.getAttrs(user);
 *     以上代码可以获取到 User 这个 model 中的 attrs 属性
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class CPI {

	/**
	 * Return the attributes map of the model
	 * @param model the model extends from class Model
	 * @return the attributes map of the model
	 */
	public static final Map<String, Object> getAttrs(Model model) {
		return model._getAttrs();
	}

	public static final Set<String> getModifyFlag(Model model) {
		return model._getModifyFlag();
	}

	public static final Set<String> getModifyFlag(Row record) {
		return record._getModifyFlag();
	}

	public static final Table getTable(Model model) {
		return model._getTable();
	}

	public static final Config getConfig(Model model) {
		return model._getWriteConfig();
	}

	public static final String getConfigName(Model model) {
		return model.configName;
	}

	public static final Class<? extends Model> getUsefulClass(Model model) {
		return model._getUsefulClass();
	}

	public static <T> List<T> query(Connection conn, String sql, Object... paras) throws SQLException {
		return Db.query(DbKit.config, conn, sql, paras);
	}

	public static <T> List<T> query(String configName, Connection conn, String sql, Object... paras) throws SQLException {
		return Db.query(DbKit.getConfig(configName), conn, sql, paras);
	}

	public static <T> List<T> query(Config config, Connection conn, String sql, Object... paras) throws SQLException {
		return Db.query(config, conn, sql, paras);
	}

	/**
	 * Return the columns map of the record
	 * @param record the Record object
	 * @return the columns map of the record
	public static final Map<String, Object> getColumns(Record record) {
		return record.getColumns();
	} */

	public static void setColumnsMap(Row record, Map<String, Object> columns) {
		record.setColumnsMap(columns);
	}

	public static List<Row> find(Connection conn, String sql, Object... paras) throws SQLException {
		return Db.find(DbKit.config, conn, sql, paras);
	}

	public static List<Row> find(String configName, Connection conn, String sql, Object... paras) throws SQLException {
		return Db.find(DbKit.getConfig(configName), conn, sql, paras);
	}

	public static Page<Row> paginate(Connection conn, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) throws SQLException {
		return Db.paginate(DbKit.config, conn, pageNumber, pageSize, select, sqlExceptSelect, paras);
	}

	public static Page<Row> paginate(String configName, Connection conn, int pageNumber, int pageSize, String select, String sqlExceptSelect, Object... paras) throws SQLException {
		return Db.paginate(DbKit.getConfig(configName), conn, pageNumber, pageSize, select, sqlExceptSelect, paras);
	}

	public static int update(Connection conn, String sql, Object... paras) throws SQLException {
		return Db.update(DbKit.config, conn, sql, paras);
	}

	public static int update(String configName, Connection conn, String sql, Object... paras) throws SQLException {
		return Db.update(DbKit.getConfig(configName), conn, sql, paras);
	}

	public static void setTablePrimaryKey(Table table, String primaryKey) {
		table.setPrimaryKey(primaryKey);
	}

	public static void addModelToConfigMapping(Class<? extends Model> modelClass, Config config) {
		DbKit.addModelToConfigMapping(modelClass, config);
	}
}

