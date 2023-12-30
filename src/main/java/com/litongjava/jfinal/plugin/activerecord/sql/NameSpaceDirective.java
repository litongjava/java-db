package com.litongjava.jfinal.plugin.activerecord.sql;

import com.jfinal.template.Directive;
import com.jfinal.template.Env;
import com.jfinal.template.TemplateException;
import com.jfinal.template.expr.ast.Const;
import com.jfinal.template.expr.ast.Expr;
import com.jfinal.template.expr.ast.ExprList;
import com.jfinal.template.io.Writer;
import com.jfinal.template.stat.ParseException;
import com.jfinal.template.stat.Scope;

/**
 * NameSpaceDirective
 */
public class NameSpaceDirective extends Directive {
	
	static final String NAME_SPACE_KEY = "_NAME_SPACE_";
	
	private String nameSpace;
	
	public void setExprList(ExprList exprList) {
		if (exprList.length() == 0) {
			throw new ParseException("The parameter of #namespace directive can not be blank", location);
		}
		if (exprList.length() > 1) {
			throw new ParseException("Only one parameter allowed for #namespace directive", location);
		}
		Expr expr = exprList.getExpr(0);
		if (expr instanceof Const && ((Const)expr).isStr()) {
		} else {
			throw new ParseException("The parameter of #namespace directive must be String", location);
		}
		
		this.nameSpace = ((Const)expr).getStr();
	}
	
	public void exec(Env env, Scope scope, Writer writer) {
		if (scope.get(NAME_SPACE_KEY) != null) {
			throw new TemplateException("#namespace directive can not be nested", location);
		}
		scope.set(NAME_SPACE_KEY, nameSpace);
		try {
			stat.exec(env, scope, writer);
		} finally {
			scope.remove(NAME_SPACE_KEY);
		}
	}
	
	public boolean hasEnd() {
		return true;
	}
}





