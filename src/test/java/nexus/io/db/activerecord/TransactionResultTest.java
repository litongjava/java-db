package nexus.io.db.activerecord;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.Collections;
import javax.sql.DataSource;
import org.junit.*;
import static org.junit.Assert.*;

public class TransactionResultTest {
  private int commits, rollbacks, closes;
  private Config config;
  private Connection connection;
  private int isolationChanges;
  private int isolation = Connection.TRANSACTION_READ_COMMITTED;

  @Before public void setup() {
    connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Connection.class},
        (p, m, a) -> {
          switch (m.getName()) {
          case "getAutoCommit": return true;
          case "getTransactionIsolation": return isolation;
          case "setTransactionIsolation":
            isolationChanges++;
            isolation = (Integer) a[0];
            return null;
          case "commit": commits++; return null;
          case "rollback": rollbacks++; return null;
          case "close": closes++; return null;
          default: return null;
          }
        });
    DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {DataSource.class},
        (p, m, a) -> m.getName().equals("getConnection") ? connection : null);
    config = new Config(DbKit.MAIN_CONFIG_NAME, source);
    DbKit.addConfig(config);
  }

  @After public void cleanup() {
    Db.initReplicas(Collections.emptyList());
    DbKit.replicaConfigs = null;
    DbKit.removeConfig(config.getName());
    DbKit.removeConfig("replica-test");
  }

  @Test public void resultNullAndFalseCommitAndRelease() {
    assertEquals("ok", Db.txResult(() -> "ok"));
    assertNull(Db.txResult(() -> null));
    assertEquals(Boolean.FALSE, Db.txResult(() -> false));
    assertEquals(3, commits); assertEquals(0, rollbacks); assertEquals(3, closes);
    assertNull(config.getThreadLocalConnection());
  }

  @Test public void checkedExceptionRollsBackAndKeepsCause() {
    Exception original = new Exception("test");
    try { Db.txResult(() -> { throw original; }); fail(); }
    catch (ActiveRecordException e) { assertSame(original, e.getCause()); }
    assertEquals(1, rollbacks); assertEquals(0, commits); assertNull(config.getThreadLocalConnection());
  }

  @Test public void nestedResultSharesConnectionAndCommitsOnce() {
    assertEquals("nested", Db.txResult(Connection.TRANSACTION_READ_COMMITTED, () -> Db.use().txResult(() -> {
      assertSame(connection, config.getThreadLocalConnection()); return "nested";
    })));
    assertEquals(1, commits); assertEquals(1, closes);
    assertEquals(1, isolationChanges);
  }

  @Test public void nestedBooleanRollbackCannotReturnSuccessResult() {
    try {
      Db.txResult(() -> { Db.tx(() -> false); return "must not be returned"; }); fail();
    } catch (ActiveRecordException expected) {
      assertTrue(expected.getMessage().contains("rolled back"));
    }
    assertEquals(1, rollbacks); assertEquals(0, commits); assertNull(config.getThreadLocalConnection());
  }

  @Test public void automaticReadRoutingStaysOnTransactionPrimary() {
    Config replica = new Config("replica-test", config.getDataSource());
    DbKit.addReplicaConfigs(Collections.singletonList(replica));
    assertSame(replica, Db.useRead().getConfig());
    Db.txResult(() -> {
      assertSame(config, Db.useRead().getConfig());
      assertSame(config, DbKit.getReadConfig()); return null;
    });
    assertSame(replica, Db.useRead().getConfig());
  }
  @Test public void runtimeExceptionKeepsIdentityAfterRollback() {
    IllegalArgumentException original = new IllegalArgumentException("business validation");
    try {
      Db.txResult(() -> Db.txResult(() -> { throw original; }));
      fail();
    } catch (IllegalArgumentException actual) {
      assertSame(original, actual);
    }
    assertEquals(1, rollbacks);
    assertEquals(0, commits);
    assertEquals(1, closes);
    assertNull(config.getThreadLocalConnection());
  }

  @Test public void errorIsNotConvertedToApplicationException() {
    AssertionError original = new AssertionError("fatal");
    try {
      Db.tx(() -> { throw original; });
      fail();
    } catch (AssertionError actual) {
      assertSame(original, actual);
    }
    assertEquals(1, rollbacks);
    assertEquals(0, commits);
    assertNull(config.getThreadLocalConnection());
  }
}
