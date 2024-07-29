package com.litongjava.db.activerecord.generator;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * TableMeta
 */
@SuppressWarnings("serial")
public class TableMeta implements Serializable {
	
	public String name;					// 表名
	public String remarks;				// 表备注
	public String primaryKey;			// 主键，复合主键以逗号分隔
	public List<ColumnMeta> columnMetas = new ArrayList<ColumnMeta>();	// 字段 meta
	
	// ---------
	
	public String baseModelName;		// 生成的 base model 名
	public String baseModelContent;		// 生成的 base model 内容
	
	public String modelName;			// 生成的 model 名
	public String modelContent;			// 生成的 model 内容
	
	// ---------
	
	public int colNameMaxLen = "Field".length();			// 字段名最大宽度，用于辅助生成字典文件样式
	public int colTypeMaxLen = "Type".length();				// 字段类型最大宽度，用于辅助生成字典文件样式
	public int colDefaultValueMaxLen = "Default".length();	// 字段默认值最大宽度，用于辅助生成字典文件样式
}




