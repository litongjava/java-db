package com.litongjava.jfinal.plugin.activerecord.tx;

/**
 * TxReadCommitted.
 */
public class TxReadCommitted extends Tx {
	
    /**
     * A constant indicating that
     * dirty reads are prevented; non-repeatable reads and phantom
     * reads can occur.  This level only prohibits a transaction
     * from reading a row with uncommitted changes in it.
     */
    private int TRANSACTION_READ_COMMITTED   = 2;
    
    @Override
	protected int getTransactionLevel(com.litongjava.jfinal.plugin.activerecord.Config config) {
		return TRANSACTION_READ_COMMITTED;
	}
}

