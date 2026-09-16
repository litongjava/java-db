package nexus.io.db.activerecord;

import java.lang.reflect.Proxy;
import com.jfinal.kit.Kv;
import java.util.*;
import javax.sql.DataSource;
import org.junit.*;
import static org.junit.Assert.*;
import org.postgresql.util.PGobject;
import nexus.io.db.activerecord.dialect.PostgreSqlDialect;

public class MapOperationsTest {
  private Config config;
  private CaptureDb db;

  private static class CaptureDb extends DbPro {
    String sql;
    Object[] parameters;
    CaptureDb() { super("map-tests"); }
    @Override public Kv findFirstMap(String sql, String[] fields, Object... paras) {
      this.sql = sql;
      this.parameters = paras;
      return Kv.by("id", 7L);
    }
    @Override public List<Kv> findMaps(String sql, String[] fields, Object... paras) {
      this.sql = sql;
      this.parameters = paras;
      return Collections.singletonList(Kv.by("id", 7L));
    }
    @Override public Long queryLong(String sql, Object... paras) { return 3L; }
    @Override public int update(String sql, Object... paras) {
      this.sql = sql;
      this.parameters = paras;
      return 1;
    }
  }

  @Before public void setup() {
    DataSource source = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {DataSource.class}, (proxy, method, args) -> { throw new AssertionError("Unexpected connection"); });
    config = new Config("map-tests", source, new PostgreSqlDialect());
    DbKit.addConfig(config);
    db = new CaptureDb();
  }
  @After public void cleanup() { DbKit.removeConfig("map-tests"); }

  @Test public void insertKeepsCallerMapAndUsesBoundJsonb() {
    Kv input = Kv.create();
    input.put("payload", Kv.by("value", "quoted'value"));
    input.put("tags", Collections.emptyList());
    assertEquals(7L, db.insertMapReturning("sample_2", input, new String[] {"payload"}).get("id"));
    assertEquals("insert into \"sample_2\" (\"payload\",\"tags\") values (?,?) returning *", db.sql);
    assertEquals(2, input.size());
    assertEquals("jsonb", ((PGobject) db.parameters[0]).getType());
    assertEquals("[]", ((PGobject) db.parameters[1]).getValue());
  }

  @Test public void updateIncludesNullAndTenantConditionsAndServerTimestamp() {
    Kv conditions = Kv.create();
    conditions.put("tenant_id", 5L);
    conditions.put("deleted_at", null);
    assertEquals(1, db.updateMapByColumns("sample", Kv.by("name", "value"), conditions, "updated_at"));
    assertTrue(db.sql.contains("\"tenant_id\"=? and \"deleted_at\" is null"));
    assertTrue(db.sql.contains("\"updated_at\"=current_timestamp"));
    assertArrayEquals(new Object[] {"value", 5L}, db.parameters);
  }

  @Test public void rejectsUnsafeIdentifiersAndUnrestrictedUpdates() {
    Kv fields = Kv.by("name", "x");
    try { db.insertMapReturning("sample;drop table sample", fields, new String[0]); fail(); }
    catch (IllegalArgumentException expected) { }
    try { db.updateMapByColumns("sample", fields, Kv.create()); fail(); }
    catch (IllegalArgumentException expected) { }
    try { db.updateMapByColumns("sample", fields, Kv.by("id", 1), "name"); fail(); }
    catch (IllegalArgumentException expected) { }
  }

  @Test public void paginationUsesLongOffsetAndPreservesOriginalParameters() {
    Object[] original = {5L};
    Kv page = db.paginateMap(Integer.MAX_VALUE, 20, "select count(*) from sample where tenant_id=?",
        "select * from sample where tenant_id=? order by id", new String[0], original);
    assertEquals(Long.valueOf(3L), page.getLong("total"));
    assertEquals(Integer.valueOf(Integer.MAX_VALUE), page.getInt("page"));
    assertArrayEquals(new Object[] {5L, 20, 42949672920L}, db.parameters);
    assertArrayEquals(new Object[] {5L}, original);
    try { db.paginateMap(0, 20, "count", "find", new String[0]); fail(); }
    catch (IllegalArgumentException expected) { }
  }

  @Test public void jsonbConversionIsExplicitAndDoesNotMutateArguments() {
    List<String> list = Collections.singletonList("a");
    Kv nested = Kv.by("amount", "123");
    Object[] original = {list, null, 3L, nested};
    Object[] converted = Db.toJsonbParameters(original);
    assertSame(list, original[0]);
    assertTrue(converted[0] instanceof PGobject);
    assertNull(converted[1]);
    assertEquals(3L, converted[2]);
    assertSame(nested, original[3]);
    assertTrue(converted[3] instanceof PGobject);
    assertEquals("jsonb", ((PGobject) converted[3]).getType());
  }
}
