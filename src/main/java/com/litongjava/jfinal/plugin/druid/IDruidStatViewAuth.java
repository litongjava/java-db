package com.litongjava.jfinal.plugin.druid;


import javax.servlet.http.HttpServletRequest;

/**
 * 授权
 */
@FunctionalInterface
public interface IDruidStatViewAuth {
	boolean isPermitted(HttpServletRequest request);
}
