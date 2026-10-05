package nexus.io.db.activerecord;

import static org.junit.Assert.*;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DbBatchLifecycleTest {
  private Config config;
  private DbPro db;
  private Connection connection;
  private boolean autoCommit = true;
  private boolean pending;
  private boolean failBatch = true;
  private boolean failRestore;
  private boolean failRollback;
  private int executions;
  private int failOnBatch = 1;
  private int isolation = Connection.TRANSACTION_READ_COMMITTED;
  private int commits;
  private int rollbacks;
  private int closes;
  private final SQLException batchFailure = new SQLException("batch failed after partial execution");

  @Before
  public void setup() {
    PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PreparedStatement.class}, (proxy, method, args) -> {
          if ("executeBatch".equals(method.getName())) {
            pending = true;
            if (failBatch && ++executions >= failOnBatch) {
              throw batchFailure;
            }
            return new int[] {1};
          }
          return null;
        });
    connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          switch (method.getName()) {
          case "getAutoCommit": return autoCommit;
          case "getTransactionIsolation": return isolation;
          case "setTransactionIsolation": isolation = (Integer) args[0]; return null;
          case "setAutoCommit":
            boolean next = (Boolean) args[0];
            if (next && failRestore) {
              throw new SQLException("restore failed");
            }
            if (next && !autoCommit && pending) {
              commits++;
              pending = false;
            }
            autoCommit = next;
            return null;
          case "prepareStatement":
          case "createStatement": return statement;
          case "commit": commits++; pending = false; return null;
          case "rollback":
            rollbacks++;
            if (failRollback) {
              throw new SQLException("rollback failed");
            }
            pending = false;
            return null;
          case "close": closes++; return null;
          default: return null;
          }
        });
    DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DataSource.class}, (proxy, method, args) -> connection);
    config = new Config("batch-lifecycle", source);
    DbKit.addConfig(config);
    db = new DbPro(config.getName());
  }

  @After
  public void cleanup() {
    config.removeThreadLocalConnection();
    DbKit.removeConfig(config.getName());
  }

  private void batch(int variant) {
    switch (variant) {
    case 0: db.batch("insert into test(value) values (?)", new Object[][] {{"a"}}, 1); break;
    case 1: db.batch("insert into test(value) values (?)", "value", Collections.singletonList(new Row().set("value", "a")), 1); break;
    case 2: db.batch("insert into test(value) values (?)", "value", new String[0], Collections.singletonList(new Row().set("value", "a")), 1); break;
    default: db.batch(Collections.singletonList("insert into test(value) values ('a')"), 1); break;
    }
  }

  @Test
  public void everyBatchOverloadRollsBackFailedPendingWorkBeforeRestoringAutoCommit() {
    for (int variant = 0; variant < 4; variant++) {
      try {
        batch(variant);
        fail("Expected batch failure");
      } catch (ActiveRecordException expected) {
        assertSame(batchFailure, expected.getCause());
      }
      assertEquals(0, commits);
      assertEquals(variant + 1, rollbacks);
      assertEquals(variant + 1, closes);
    }
  }

  @Test
  public void cleanupFailureDoesNotHideBatchFailureOrLeakConnection() {
    failRestore = true;
    try {
      batch(0);
      fail("Expected batch failure");
    } catch (ActiveRecordException expected) {
      assertSame(batchFailure, expected.getCause());
    }
    assertEquals(1, closes);
    assertEquals(1, rollbacks);
  }

  @Test
  public void failedRollbackNeverEnablesAutoCommitOnPendingWork() {
    failRollback = true;
    try {
      batch(0);
      fail("Expected batch failure");
    } catch (ActiveRecordException expected) {
      assertSame(batchFailure, expected.getCause());
    }
    assertEquals(0, commits);
    assertFalse(autoCommit);
    assertEquals(1, closes);
  }

  @Test
  public void nestedBatchLeavesRollbackAndConnectionOwnershipToOuterTransaction() {
    config.setThreadLocalConnection(connection);
    autoCommit = false;
    try {
      batch(0);
      fail("Expected batch failure");
    } catch (ActiveRecordException expected) {
      assertSame(batchFailure, expected.getCause());
    }
    assertEquals(0, commits);
    assertEquals(0, rollbacks);
    assertEquals(0, closes);
    assertSame(connection, config.getThreadLocalConnection());
  }

  @Test
  public void successfulBatchPreservesChunkCommitBehavior() {
    failBatch = false;
    batch(0);
    assertEquals(1, commits);
    assertEquals(0, rollbacks);
    assertEquals(1, closes);
    assertTrue(autoCommit);
  }

  @Test
  public void transactionAlwaysClosesEvenWhenRestoringAutoCommitFails() {
    failRestore = true;
    assertTrue(db.tx(() -> true));
    assertEquals(1, commits);
    assertEquals(1, closes);
    assertNull(config.getThreadLocalConnection());
  }

  @Test
  public void priorCommittedChunksRemainCommittedWhenLaterChunkFails() {
    failOnBatch = 2;
    try {
      db.batch("insert into test(value) values (?)", new Object[][] {{"a"}, {"b"}}, 1);
      fail("Expected second chunk failure");
    } catch (ActiveRecordException expected) {
      assertSame(batchFailure, expected.getCause());
    }
    assertEquals(1, commits);
    assertEquals(1, rollbacks);
    assertEquals(1, closes);
  }

  @Test
  public void transactionRestoresIsolationBeforeReturningConnection() {
    assertTrue(db.tx(Connection.TRANSACTION_SERIALIZABLE, () -> {
      assertEquals(Connection.TRANSACTION_SERIALIZABLE, isolation);
      return true;
    }));
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, isolation);
    assertEquals(1, closes);
  }

  @Test
  public void transactionRollbackFailureKeepsOriginalErrorAndAvoidsImplicitCommit() {
    failRollback = true;
    IllegalStateException original = new IllegalStateException("business failure");
    try {
      db.tx(() -> {
        pending = true;
        throw original;
      });
      fail("Expected original failure");
    } catch (IllegalStateException expected) {
      assertSame(original, expected);
      assertEquals(1, expected.getSuppressed().length);
    }
    assertEquals(0, commits);
    assertEquals(1, closes);
    assertFalse(autoCommit);
    assertNull(config.getThreadLocalConnection());
  }
}
