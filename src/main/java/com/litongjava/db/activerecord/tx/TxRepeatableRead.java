package com.litongjava.db.activerecord.tx;

/**
 * TxRepeatableRead.
 */
public class TxRepeatableRead extends Tx {
	
    /**
     * A constant indicating that
     * dirty reads and non-repeatable reads are prevented; phantom
     * reads can occur.  This level prohibits a transaction from
     * reading a row with uncommitted changes in it, and it also
     * prohibits the situation where one transaction reads a row,
     * a second transaction alters the row, and the first transaction
     * rereads the row, getting different values the second time
     * (a "non-repeatable read").
     */
	private int TRANSACTION_REPEATABLE_READ  = 4;
    
	@Override
	protected int getTransactionLevel(com.litongjava.db.activerecord.Config config) {
		return TRANSACTION_REPEATABLE_READ;
	}
}




