package com.litongjava.db.activerecord.tx;

import java.sql.Connection;
import java.sql.SQLException;

import com.litongjava.jfinal.aop.Invocation;

/**
 * 支持定制事务行为，否则 Tx 拦截器只会在抛出异常时回滚事务
 *
 * <pre>
 * 例如通过返回值 Ret 对象来决定事务的提交与回滚：
 *   Tx.setTxFun((inv, conn) -> {
 *       inv.invoke();
 *
 *       // 根据业务层返回值 Ret 对象的状态决定提交与回滚
 *       Object retValue = inv.getReturnValue();
 *       if (retValue instanceof Ret) {
 *           Ret ret = (Ret)retValue;
 *           if (ret.isOk()) {
 *               conn.commit();
 *           } else {
 *               conn.rollback();
 *           }
 *           return ;
 *       }
 *
 *       // 返回其它类型值的情况
 *       conn.commit();
 *    });
 * </pre>
 */
@FunctionalInterface
public interface TxFun {
    void call(Invocation inv, Connection conn) throws SQLException;
}



