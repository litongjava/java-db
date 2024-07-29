package com.litongjava.db.activerecord;


import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import com.jfinal.kit.StrKit;

/**
 * Table save the table meta info like column name and column type.
 */
public class Table {
	
	private String name;
	private String[] primaryKey = null;
	private Map<String, Class<?>> columnTypeMap;	// config.containerFactory.getAttrsMap();
	
	private Class<? extends Model<?>> modelClass;
	
	public Table(String name, Class<? extends Model<?>> modelClass) {
		if (StrKit.isBlank(name))
			throw new IllegalArgumentException("Table name can not be blank.");
		if (modelClass == null)
			throw new IllegalArgumentException("Model class can not be null.");
		
		this.name = name.trim();
		this.modelClass = modelClass;
	}
	
	public Table(String name, String primaryKey, Class<? extends Model<?>> modelClass) {
		if (StrKit.isBlank(name))
			throw new IllegalArgumentException("Table name can not be blank.");
		if (StrKit.isBlank(primaryKey))
			throw new IllegalArgumentException("Primary key can not be blank.");
		if (modelClass == null)
			throw new IllegalArgumentException("Model class can not be null.");
		
		this.name = name.trim();
		setPrimaryKey(primaryKey.trim());
		this.modelClass = modelClass;
	}
	
	void setPrimaryKey(String primaryKey) {
		String[] arr = primaryKey.split(",");
		for (int i=0; i<arr.length; i++)
			arr[i] = arr[i].trim();
		this.primaryKey = arr;
	}
	
	public void setColumnTypeMap(Map<String, Class<?>> columnTypeMap) {
		if (columnTypeMap == null)
			throw new IllegalArgumentException("columnTypeMap can not be null");
		
		this.columnTypeMap = columnTypeMap;
	}
	
	public String getName() {
		return name;
	}
	
	public void setColumnType(String columnLabel, Class<?> columnType) {
		columnTypeMap.put(columnLabel, columnType);
	}
	
	public Class<?> getColumnType(String columnLabel) {
		return columnTypeMap.get(columnLabel);
	}
	
	/**
	 * Model.save() need know what columns belongs to himself that he can saving to db.
	 * Think about auto saving the related table's column in the future.
	 */
	public boolean hasColumnLabel(String columnLabel) {
	    // TreeMap.containsKey(...) 不允许参数为 null，故需添加 null 值判断
		return columnLabel != null && columnTypeMap.containsKey(columnLabel);
	}
	
	/**
	 * update() and delete() need this method.
	 */
	public String[] getPrimaryKey() {
		return primaryKey;
	}
	
	public Class<? extends Model<?>> getModelClass() {
		return modelClass;
	}
	
	public Map<String, Class<?>> getColumnTypeMap() {
		return Collections.unmodifiableMap(columnTypeMap);
	}
	
	public Set<Entry<String, Class<?>>> getColumnTypeMapEntrySet() {
		return Collections.unmodifiableSet(columnTypeMap.entrySet());
	}
	
	public Set<String> getColumnNameSet() {
		return Collections.unmodifiableSet(columnTypeMap.keySet());
	}
}





