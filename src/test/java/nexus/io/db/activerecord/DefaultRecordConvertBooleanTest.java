package nexus.io.db.activerecord;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import org.junit.Test;
import nexus.io.db.activerecord.bean.DefaultRecordConvert;

public class DefaultRecordConvertBooleanTest {
  public static class Flags {
    public Boolean enabled;
    public boolean active;
  }

  @Test
  public void preservesNativeJdbcBooleans() {
    assertFlags(Boolean.TRUE, true);
    assertFlags(Boolean.FALSE, false);
  }

  @Test
  public void supportsNumericJdbcTypes() {
    for (Number zero : new Number[] {0, 0L, (short) 0, (byte) 0, 0.0, BigDecimal.ZERO}) {
      assertFlags(zero, false);
    }
    for (Number nonzero : new Number[] {1, 1L, (short) 1, (byte) 1, -1, new BigDecimal("0.5")}) {
      assertFlags(nonzero, true);
    }
  }

  @Test
  public void nullRetainsJavaDefaults() {
    Flags result = new DefaultRecordConvert().toJavaBean(new Row(), Flags.class);
    assertNull(result.enabled);
    assertFalse(result.active);
  }

  private void assertFlags(Object value, boolean expected) {
    Row row = Row.by("enabled", value).set("active", value);
    Flags result = new DefaultRecordConvert().toJavaBean(row, Flags.class);
    assertEquals(Boolean.valueOf(expected), result.enabled);
    assertEquals(expected, result.active);
  }
}
