package nexus.io.db.activerecord;

import static org.junit.Assert.*;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.Test;
import com.jfinal.kit.Kv;
import nexus.io.kit.RowUtils;
import nexus.io.kit.PgObjectUtils;
import nexus.io.db.utils.HtmlTableUtils;
import nexus.io.db.utils.MarkdownTableUtils;

public class RowPresentationTest {
  @Test
  public void convertingToKvDoesNotChangeOriginalNumericTypes() {
    Row row = new Row().set("user_id", 9L).set("large", new BigInteger("99999999999999999999"));
    for (boolean camel : new boolean[] {false, true}) {
      Kv result = RowUtils.toKv(row, camel);
      assertEquals("9", result.get(camel ? "userId" : "user_id"));
      assertEquals("99999999999999999999", result.get("large"));
      assertTrue(row.get("user_id") instanceof Long);
      assertTrue(row.get("large") instanceof BigInteger);
      result.set("large", "changed");
      assertEquals(new BigInteger("99999999999999999999"), row.get("large"));
    }
  }

  private Row orderedRow() {
    Row row = new Row();
    row.setColumnsMap(new LinkedHashMap<>());
    return row;
  }

  @Test
  public void tableValuesFollowHeaderOrderAndMissingColumnsRemainEmpty() {
    Row first = orderedRow().set("id", 1).set("name", "first");
    Row second = orderedRow().set("name", "second").set("id", 2);
    Row third = orderedRow().set("name", "third").set("extra", "ignored");
    List<Row> rows = Arrays.asList(first, second, third);
    List<List<Object>> data = RowUtils.getListData(rows, rows.size());
    assertEquals(Arrays.asList(2, "second"), data.get(1));
    assertEquals(Arrays.asList(null, "third"), data.get(2));
    assertTrue(MarkdownTableUtils.to(rows).contains("| 2 | second |"));
    assertTrue(HtmlTableUtils.to(rows).contains("<td>2</td>\n      <td>second</td>"));
  }

  @Test
  public void databaseJsonTextIsNotQuotedASecondTimeForPresentation() {
    Row row = orderedRow().set("payload", PgObjectUtils.jsonb("{\"name\":\"test\"}"));
    assertEquals("{\"name\":\"test\"}", RowUtils.getListData(Arrays.asList(row), 1).get(0).get(0));
  }
}
