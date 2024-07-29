package com.litongjava.db.activerecord;

/**
 * IDbProFactory
 * 
 * 用于自义扩展 DbPro 实现类，实现定制化功能
 * 1：创建 DbPro 继承类： public class MyDbPro extends DbPro
 * 2：创建 IDbProFactory 实现类：public class MyDbProFactory implements IDbProFactory，让其 getDbPro 方法 返回 MyDbPro 对象
 * 3：配置生效： activeRecordPlugin.setDbProFactory(new MyDbProFactory())
 * 
 * 注意：每个 ActiveRecordPlugin 对象拥有独立的 IDbProFactory 对象，多数据源使用时注意要对每个 arp 进行配置
 */
@FunctionalInterface
public interface IDbProFactory {
	
	DbPro getDbPro(String configName);
	
	static final IDbProFactory defaultDbProFactory = new IDbProFactory() {
		public DbPro getDbPro(String configName) {
			return new DbPro(configName);
		}
	};
}


