package nexus.io.kit;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PGobject;

import com.jfinal.kit.Kv;

import nexus.io.db.activerecord.Row;
import nexus.io.tio.utils.json.JsonUtils;
import nexus.io.tio.utils.name.CamelNameUtils;

public class RowUtils {

  public static boolean isExistsPGobject = true;
  static {
    try {
      Class.forName("org.postgresql.util.PGobject");
    } catch (ClassNotFoundException e) {
      isExistsPGobject = false;
    }
  }

  public static List<List<Object>> getListData(List<Row> records, int size) {
    List<List<Object>> columnValues = new ArrayList<>(size);
    String[] columns = size > 0 ? records.get(0).getColumnNames() : new String[0];
    for (int i = 0; i < size; i++) {
      Object[] columnValuesForRow = new Object[columns.length];
      for (int j = 0; j < columnValuesForRow.length; j++) {
        columnValuesForRow[j] = records.get(i).get(columns[j]);
        if (columnValuesForRow[j] instanceof BigInteger) {
          columnValuesForRow[j] = columnValuesForRow[j].toString();
        } else if (columnValuesForRow[j] instanceof Map) {
          columnValuesForRow[j] = JsonUtils.toJson(columnValuesForRow[j]);
        } else if (columnValuesForRow[j] instanceof List) {
          columnValuesForRow[j] = JsonUtils.toJson(columnValuesForRow[j]);
        } else if (isExistsPGobject && columnValuesForRow[j] instanceof PGobject) {
          PGobject pgObject = (PGobject) columnValuesForRow[j];
          columnValuesForRow[j] = pgObject.getValue();
        }
      }
      List<Object> asList = Arrays.asList(columnValuesForRow);
      columnValues.add(asList);
    }
    return columnValues;
  }

  public static List<Kv> toKv(List<Row> list, boolean underscoreToCamel) {
    List<Kv> result = new ArrayList<>(list.size());
    for (Row row : list) {
      result.add(toKv(row, underscoreToCamel));
    }
    return result;
  }

  public static Kv toKv(Row record, boolean underscoreToCamel) {
    if (record == null) {
      return null;
    }
    Kv result = Kv.create();
    // Normalize presentation values without modifying the source record.
    for (Map.Entry<String, Object> entry : record.toMap().entrySet()) {
      Object value = entry.getValue();
      if (value instanceof Long || value instanceof BigInteger) {
        value = value.toString();
      }
      String key = underscoreToCamel ? CamelNameUtils.toCamel(entry.getKey()) : entry.getKey();
      result.set(key, value);
    }
    return result;
  }

  public static List<Map<String, Object>> toMap(List<Row> records) {
    List<Map<String, Object>> list = new ArrayList<>(records.size());
    for (Row row : records) {
      list.add(row.toMap());
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  public static Kv underscoreToCamel(Map<String, Object> map) {
    Kv kv = new Kv();
    map.forEach((key, value) -> kv.put(CamelNameUtils.toCamel(key), value));
    return kv;
  }
}
