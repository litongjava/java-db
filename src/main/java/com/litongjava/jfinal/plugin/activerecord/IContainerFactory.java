package com.litongjava.jfinal.plugin.activerecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("rawtypes")
public interface IContainerFactory {
	Map getAttrsMap();
	Map getColumnsMap();
	Set getModifyFlagSet();
	
	static final IContainerFactory defaultContainerFactory = new IContainerFactory() {
		
		public Map<String, Object> getAttrsMap() {
			return new HashMap<String, Object>();
		}
		
		public Map<String, Object> getColumnsMap() {
			return new HashMap<String, Object>();
		}
		
		public Set<String> getModifyFlagSet() {
			return new HashSet<String>();
		}
	};
}
