package com.litongjava.jfinal.plugin.activerecord.tx;

/**
 * TxSerializable.
 */
public class TxSerializable extends Tx {
	
    /**
     * A constant indicating that
     * dirty reads, non-repeatable reads and phantom reads are prevented.
     * This level includes the prohibitions in
     * <code>TRANSACTION_REPEATABLE_READ</code> and further prohibits the 
     * situation where one transaction reads all rows that satisfy
     * a <code>WHERE</code> condition, a second transaction inserts a row that
     * satisfies that <code>WHERE</code> condition, and the first transaction
     * rereads for the same condition, retrieving the additional
     * "phantom" row in the second read.
     */
    private int TRANSACTION_SERIALIZABLE     = 8;
    
    @Override
	protected int getTransactionLevel(com.litongjava.jfinal.plugin.activerecord.Config config) {
		return TRANSACTION_SERIALIZABLE;
	}
}



