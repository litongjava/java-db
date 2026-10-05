package nexus.io.db.activerecord;

import static org.junit.Assert.*;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DbQueryResourcesTest {
  private Config config;
  private DbPro db;
  private Connection connection;
  private int statementCloses;
  private int resultCloses;
  private int connectionCloses;
  private int rows;
  private String failAt;
  private boolean failClose;
  private final SQLException failure = new SQLException("query failure");

  @Before
  public void setup() {
    ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {ResultSetMetaData.class}, (proxy, method, args) -> {
          switch (method.getName()) {
          case "getColumnCount": return 1;
          case "getColumnType": return Types.VARBINARY;
          case "getColumnLabel":
          case "getColumnName": return "payload";
          default: return null;
          }
        });
    ResultSet result = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {ResultSet.class}, (proxy, method, args) -> {
          switch (method.getName()) {
          case "getMetaData": return metadata;
          case "next":
            if ("read".equals(failAt)) {
              throw failure;
            }
            return rows++ == 0;
          case "getBytes":
          case "getObject": return new byte[] {1, 2};
          case "close":
            resultCloses++;
            if (failClose) {
              throw new SQLException("result close failed");
            }
            return null;
          default: return null;
          }
        });
    PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PreparedStatement.class}, (proxy, method, args) -> {
          if (method.getName().startsWith("set") && "bind".equals(failAt)) {
            throw failure;
          }
          if ("executeQuery".equals(method.getName())) {
            if ("execute".equals(failAt)) {
              throw failure;
            }
            rows = 0;
            return result;
          }
          if ("close".equals(method.getName())) {
            statementCloses++;
            if (failClose) {
              throw new SQLException("statement close failed");
            }
          }
          return null;
        });
    connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {Connection.class}, (proxy, method, args) -> {
          if ("prepareStatement".equals(method.getName())) {
            return statement;
          }
          if ("close".equals(method.getName())) {
            connectionCloses++;
          }
          return null;
        });
    DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DataSource.class}, (proxy, method, args) -> connection);
    config = new Config("query-resources", source);
    DbKit.addConfig(config);
    db = new DbPro(config.getName());
  }

  @After
  public void cleanup() {
    config.removeThreadLocalConnection();
    DbKit.removeConfig(config.getName());
  }

  private List<?> query(int variant) {
    switch (variant) {
    case 0: return db.queryListBytes(config, connection, "select payload from test where id=?", 1);
    case 1: return db.find(config, connection, "select payload from test where id=?", 1);
    case 2: return db.find(config, connection, "test", "payload", new Row().set("id", 1));
    default: return db.findByField(config, connection, "test", "payload", "id", 1);
    }
  }

  @Test
  public void successfulQueriesCloseResultsAndStatementsButNotCallerConnection() {
    for (int variant = 0; variant < 4; variant++) {
      assertEquals(1, query(variant).size());
      assertEquals(variant + 1, resultCloses);
      assertEquals(variant + 1, statementCloses);
      assertEquals(0, connectionCloses);
    }
  }

  @Test
  public void bindingAndExecutionFailureStillCloseStatements() {
    for (String stage : new String[] {"bind", "execute"}) {
      failAt = stage;
      for (int variant = 0; variant < 4; variant++) {
        int before = statementCloses;
        try {
          query(variant);
          fail("Expected query failure");
        } catch (ActiveRecordException expected) {
          assertSame(failure, expected.getCause());
        }
        assertEquals(before + 1, statementCloses);
        assertEquals(0, resultCloses);
      }
    }
  }

  @Test
  public void cleanupFailuresDoNotMaskReadingFailureOrSkipStatementClose() {
    failAt = "read";
    failClose = true;
    for (int variant = 0; variant < 4; variant++) {
      int suppressed = failure.getSuppressed().length;
      try {
        query(variant);
        fail("Expected query failure");
      } catch (ActiveRecordException expected) {
        assertSame(failure, expected.getCause());
        assertEquals(suppressed + 2, failure.getSuppressed().length);
      }
      assertEquals(variant + 1, resultCloses);
      assertEquals(variant + 1, statementCloses);
    }
  }

  @Test
  public void publicByteQueryKeepsTransactionConnectionOpen() {
    config.setThreadLocalConnection(connection);
    assertEquals(1, db.queryListBytes("select payload from test").size());
    assertEquals(1, resultCloses);
    assertEquals(1, statementCloses);
    assertEquals(0, connectionCloses);
    assertSame(connection, config.getThreadLocalConnection());
  }
}
