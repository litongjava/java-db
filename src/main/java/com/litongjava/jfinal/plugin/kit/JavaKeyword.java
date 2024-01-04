package com.litongjava.jfinal.plugin.kit;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * JavaKeyword.
 */
public class JavaKeyword {
	
	private String[] keywordArray = {
		"abstract",
		"assert",
		"boolean",
		"break",
		"byte",
		"case",
		"catch",
		"char",
		"class",
		"const",
		"continue",
		"default",
		"do",
		"double",
		"else",
		"enum",
		"extends",
		"final",
		"finally",
		"float",
		"for",
		"goto",
		"if",
		"implements",
		"import",
		"instanceof",
		"int",
		"interface",
		"long",
		"native",
		"new",
		"package",
		"private",
		"protected",
		"public",
		"return",
		"strictfp",
		"short",
		"static",
		"super",
		"switch",
		"synchronized",
		"this",
		"throw",
		"throws",
		"transient",
		"try",
		"void",
		"volatile",
		"while"
	};
	
	private Set<String> set;
	
	public static final JavaKeyword me = createSharedInstance();
	
	private static JavaKeyword createSharedInstance() {
		JavaKeyword jk = new JavaKeyword();
		jk.set = Collections.unmodifiableSet(jk.set);	// 共享对象不让修改
		return jk;
	}
	
	public JavaKeyword() {
		set = new HashSet<String>();
		for (String keyword : keywordArray) {
			set.add(keyword);
		}
	}
	
	public JavaKeyword addKeyword(String keyword) {
		if (StrKit.notBlank(keyword)) {
			set.add(keyword);
		}
		return this;
	}
	
	public JavaKeyword removeKeyword(String keyword) {
		set.remove(keyword);
		return this;
	}
	
	public boolean contains(String str) {
		return set.contains(str);
	}
}






