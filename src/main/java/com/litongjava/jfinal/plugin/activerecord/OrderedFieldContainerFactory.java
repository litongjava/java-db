package com.litongjava.jfinal.plugin.activerecord;


import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 用于支持查询出来的字段次序与 select a, b, c... 的次序一致
 * 
 * 通常用于查询类系统，字段是不确定的，字段显示的次序要与 select
 * 字句保持一致
 * 
 * 用法：
 * arp.setContainerFactory(new OrderedFieldContainerFactory())
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class OrderedFieldContainerFactory implements IContainerFactory {
	
	public Map<String, Object> getAttrsMap() {
		return new LinkedHashMap();
	}
	
	public Map<String, Object> getColumnsMap() {
		return new LinkedHashMap();
	}
	
	public Set<String> getModifyFlagSet() {
		return new HashSet();
	}
}
