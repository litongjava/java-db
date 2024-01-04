package com.litongjava.jfinal.plugin.json;

import java.util.Objects;

/**
 * json string 与 object 互转抽象
 */
public abstract class Json {
	
	private static IJsonFactory defaultJsonFactory = new JFinalJsonFactory();
	
	/**
	 * 当对象级的 datePattern 为 null 时使用 defaultDatePattern
	 * jfinal 2.1 版本暂定 defaultDatePattern 值为 null，即 jackson、fastjson
	 * 默认使用自己的 date 转换策略
	 */
	private static String defaultDatePattern = "yyyy-MM-dd HH:mm:ss";	// null;
	
	/**
	 * Json 继承类优先使用对象级的属性 datePattern, 然后才是全局性的 defaultDatePattern
	 */
	protected String datePattern = null;
	
	static void setDefaultJsonFactory(IJsonFactory defaultJsonFactory) {
		Objects.requireNonNull(defaultJsonFactory, "defaultJsonFactory can not be null");
		Json.defaultJsonFactory = defaultJsonFactory;
	}
	
	static void setDefaultDatePattern(String defaultDatePattern) {
		Json.defaultDatePattern = defaultDatePattern;
	}
	
	public Json setDatePattern(String datePattern) {
		this.datePattern = datePattern;
		return this;
	}
	
	public String getDatePattern() {
		return datePattern;
	}
	
	public String getDefaultDatePattern() {
		return defaultDatePattern;
	}
	
	public static Json getJson() {
		return defaultJsonFactory.getJson();
	}
	
	public abstract String toJson(Object object);
	
	public abstract <T> T parse(String jsonString, Class<T> type);
}




