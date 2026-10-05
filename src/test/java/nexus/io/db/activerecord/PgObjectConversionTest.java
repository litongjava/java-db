package nexus.io.db.activerecord;

import static org.junit.Assert.*;
import java.util.List;
import org.junit.Test;
import org.postgresql.util.PGobject;
import com.jfinal.kit.Kv;
import nexus.io.kit.PgObjectUtils;

public class PgObjectConversionTest {
  public static class Setting {
    private String name;
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
  }

  @Test
  public void kvConversionSupportsNullStringsAndAlreadyConvertedBeans() {
    Kv kv = Kv.create();
    PgObjectUtils.toBean(kv, "setting", Setting.class);
    assertNull(kv.get("setting"));
    kv.set("setting", "{\"name\":\"test\"}");
    PgObjectUtils.toBean(kv, "setting", Setting.class);
    Setting setting = kv.getAs("setting");
    assertEquals("test", setting.getName());
    PgObjectUtils.toBean(kv, "setting", Setting.class);
    assertSame(setting, kv.get("setting"));
  }

  @Test
  public void blankBeanValuesBecomeNullInsteadOfAnUnrelatedMap() {
    Row row = new Row().set("setting", PgObjectUtils.jsonb(" "));
    PgObjectUtils.toBean(row, "setting", Setting.class);
    assertNull(row.get("setting"));
    Kv kv = Kv.create().set("setting", PgObjectUtils.jsonb(""));
    PgObjectUtils.toBean(kv, "setting", Setting.class);
    assertNull(kv.get("setting"));
  }

  @Test
  public void blankListFieldsBecomeListsAndSqlNullStaysNull() {
    for (Object value : new Object[] {"", PgObjectUtils.jsonb(" ")}) {
      Row row = new Row().set("items", value);
      PgObjectUtils.toListBean(row, "items", Setting.class);
      assertTrue(row.get("items") instanceof List);
      assertTrue(((List<?>) row.get("items")).isEmpty());
      row.set("items", value);
      PgObjectUtils.toListMap(row, "items");
      assertTrue(row.get("items") instanceof List);
      assertTrue(((List<?>) row.get("items")).isEmpty());
    }
    Row row = new Row().set("items", null);
    PgObjectUtils.toListBean(row, "items", Setting.class);
    assertNull(row.get("items"));
  }

  @Test
  public void directConversionAcceptsNullAndPreservesValidData() {
    assertNull(PgObjectUtils.toBean((PGobject) null, Setting.class));
    assertNull(PgObjectUtils.toListBean((PGobject) null, Setting.class));
    assertEquals("test", PgObjectUtils.toBean(PgObjectUtils.jsonb("{\"name\":\"test\"}"), Setting.class).getName());
    assertEquals(1, PgObjectUtils.toListBean(PgObjectUtils.jsonb("[{\"name\":\"test\"}]"), Setting.class).size());
  }
}
