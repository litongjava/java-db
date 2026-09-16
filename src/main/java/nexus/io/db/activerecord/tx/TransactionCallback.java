package nexus.io.db.activerecord.tx;

/** A transaction that returns application data. Throw an exception to roll back. */
@FunctionalInterface
public interface TransactionCallback<T> {
  T run() throws Exception;
}
