package nexus.io.db.activerecord;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.sql.*;
import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;
import org.postgresql.util.PGobject;
import nexus.io.db.activerecord.dialect.PostgreSqlDialect;

public class PostgreSqlParametersTest {
  @Test public void primitiveArraysAreBoxedForJdbc() throws Exception {
    Object[][] cases = { {new int[] {1, 2}, "integer", new Integer[] {1, 2}},
        {new long[] {3}, "bigint", new Long[] {3L}}, {new short[] {4}, "smallint", new Short[] {4}},
        {new double[] {1.5}, "float8", new Double[] {1.5}}, {new float[] {2.5f}, "float4", new Float[] {2.5f}},
        {new boolean[] {true}, "boolean", new Boolean[] {true}}, {new int[0], "integer", new Integer[0]} };
    for (Object[] item : cases) {
      boolean[] created = {false};
      Connection c = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Connection.class},
          (p, m, a) -> {
            assertEquals("createArrayOf", m.getName()); assertEquals(item[1], a[0]);
            assertArrayEquals((Object[]) item[2], (Object[]) a[1]); created[0] = true; return null;
          });
      PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class<?>[] {PreparedStatement.class}, (p, m, a) -> m.getName().equals("getConnection") ? c : null);
      new PostgreSqlDialect().fillStatement(statement, new Object[] {item[0]});
      assertTrue(created[0]);
    }
  }

  @Test public void emptyListBindsAsEmptyJsonArray() throws Exception {
    Object[] bound = {null};
    PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PreparedStatement.class}, (p, m, a) -> { bound[0] = a[1]; return null; });
    new PostgreSqlDialect().fillStatement(statement, new Object[] {Collections.emptyList()});
    assertEquals("jsonb", ((PGobject) bound[0]).getType());
    assertEquals("[]", ((PGobject) bound[0]).getValue());
  }
  @Test public void instantBindsAsTimestampWithNanoseconds() throws Exception {
    Instant instant = Instant.parse("2026-10-07T00:00:00.123456789Z");
    Object[] bound = {null};
    PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {PreparedStatement.class}, (proxy, method, args) -> {
          assertEquals("setTimestamp", method.getName());
          bound[0] = args[1];
          return null;
        });
    new PostgreSqlDialect().fillStatement(statement, instant);
    assertEquals(instant, ((Timestamp) bound[0]).toInstant());
    new PostgreSqlDialect().fillStatement(statement, Collections.singletonList(instant));
    assertEquals(instant, ((Timestamp) bound[0]).toInstant());
  }
}
