package com.litongjava.kit;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.postgresql.util.PGobject;

import com.jfinal.kit.Kv;
import com.litongjava.db.activerecord.Row;
import com.litongjava.tio.utils.json.JsonUtils;
import com.litongjava.tio.utils.name.CamelNameUtils;

public class RowUtils {

  public static List<List<Object>> getListData(List<Row> records, int size) {
    List<List<Object>> columnValues = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      Object[] columnValuesForRow = records.get(i).getColumnValues();
      for (int j = 0; j < columnValuesForRow.length; j++) {
        if (columnValuesForRow[j] instanceof BigInteger) {
          columnValuesForRow[j] = columnValuesForRow[j].toString();
        } else if (columnValuesForRow[j] instanceof Map) {
          columnValuesForRow[j] = JsonUtils.toJson(columnValuesForRow[j]);
        } else if (columnValuesForRow[j] instanceof List) {
          columnValuesForRow[j] = JsonUtils.toJson(columnValuesForRow[j]);
        } else if (columnValuesForRow[j] instanceof PGobject) {
          PGobject pgObject = (PGobject) columnValuesForRow[j];
          columnValuesForRow[j] = JsonUtils.toJson(pgObject.getValue());
        }
      }
      List<Object> asList = Arrays.asList(columnValuesForRow);
      columnValues.add(asList);
    }
    return columnValues;
  }

  @SuppressWarnings("unchecked")
  public static Kv underscoreToCamel(Map<String, Object> map) {
    Kv kv = new Kv();
    map.forEach((key, value) -> kv.put(CamelNameUtils.toCamel(key), value));
    return kv;
  }

  public static List<Kv> recordsToKv(List<Row> list, boolean underscoreToCamel) {
    return list.stream().map(record -> {
      return recordToKv(record, underscoreToCamel);
    }).collect(Collectors.toList());
  }

  public static Kv recordToKv(Row record, boolean underscoreToCamel) {
    if (record == null) {
      return null;
    }
    Map<String, Object> map = record.toMap();
    // 将Long转为String
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() instanceof Long) {
        map.put(entry.getKey(), Long.toString((Long) entry.getValue()));
      }

      if (entry.getValue() instanceof BigInteger) {
        map.put(entry.getKey(), entry.getValue().toString());
      }
    }
    if (underscoreToCamel) {
      return underscoreToCamel(map);
    } else {
      return Kv.create().set(map);
    }
  }

  public static List<Map<String, Object>> recordsToMap(List<Row> records) {
    return records.stream().map(record -> record.toMap()).collect(Collectors.toList());
  }
}
