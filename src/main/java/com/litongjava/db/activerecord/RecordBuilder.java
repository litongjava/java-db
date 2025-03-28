package com.litongjava.db.activerecord;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.postgresql.util.PGobject;

import com.litongjava.db.activerecord.builder.BuilderKit;

/**
 * RecordBuilder.
 */
public class RecordBuilder {

  public static final RecordBuilder me = new RecordBuilder();

  public List<Row> build(Config config, ResultSet rs) throws SQLException {
    return build(config, rs, null);
  }

  public List<Row> buildJsonFields(Config config, ResultSet rs, String[] jsonFields) throws SQLException {
    return buildJsonFields(config, rs, jsonFields, null);
  }

  @SuppressWarnings("unchecked")
  public List<Row> build(Config config, ResultSet rs, Function<Row, Boolean> func) throws SQLException {
    List<Row> result = new ArrayList<Row>();
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    String[] labelNames = new String[columnCount + 1];
    int[] types = new int[columnCount + 1];
    buildLabelNamesAndTypes(rsmd, labelNames, types);
    while (rs.next()) {
      Row record = new Row();
      record.setColumnsMap(config.containerFactory.getColumnsMap());
      Map<String, Object> columns = record.getColumns();
      for (int i = 1; i <= columnCount; i++) {
        Object value = BuilderKit.getColumnValue(types, rs, i);
        // Object value = getFieldValue(rs, types, i);

        columns.put(labelNames[i], value);
      }

      if (func == null) {
        result.add(record);
      } else {
        if (!func.apply(record)) {
          break;
        }
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  public List<Row> buildJsonFields(Config config, ResultSet rs, String[] jsonFields, Function<Row, Boolean> func) throws SQLException {
    List<Row> result = new ArrayList<>();

    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    String[] labelNames = new String[columnCount + 1];
    int[] types = new int[columnCount + 1];
    buildLabelNamesAndTypes(rsmd, labelNames, types);
    while (rs.next()) {
      Row record = new Row();
      record.setColumnsMap(config.containerFactory.getColumnsMap());
      Map<String, Object> columns = record.getColumns();
      for (int i = 1; i <= columnCount; i++) {
        String labelName = labelNames[i];
        Object value = BuilderKit.getColumnValue(types, rs, i);

        // add suport for mysql
        for (String jsonField : jsonFields) {
          if (labelName.equals(jsonField) && value != null) {
            if (value instanceof String) {
              String stringValue = (String) value;
              value = BuilderKit.parseJsonField(stringValue);
            } else if (value instanceof PGobject) {
              String stringValue = ((PGobject) value).getValue();
              value = BuilderKit.parseJsonField(stringValue);
            }
          }
        }
        columns.put(labelName, value);
      }

      if (func == null) {
        result.add(record);
      } else {
        if (!func.apply(record)) {
          break;
        }
      }
    }
    return result;
  }

  public void buildLabelNamesAndTypes(ResultSetMetaData rsmd, String[] labelNames, int[] types) throws SQLException {
    for (int i = 1; i < labelNames.length; i++) {
      // 备忘：getColumnLabel 获取 sql as 子句指定的名称而非字段真实名称
      labelNames[i] = rsmd.getColumnLabel(i);
      types[i] = rsmd.getColumnType(i);
    }
  }

  /*
   * backup before use columnType static final List<Record> build(ResultSet rs)
   * throws SQLException { List<Record> result = new ArrayList<Record>();
   * ResultSetMetaData rsmd = rs.getMetaData(); int columnCount =
   * rsmd.getColumnCount(); String[] labelNames = getLabelNames(rsmd,
   * columnCount); while (rs.next()) { Record record = new Record(); Map<String,
   * Object> columns = record.getColumns(); for (int i=1; i<=columnCount; i++) {
   * Object value = rs.getObject(i); columns.put(labelNames[i], value); }
   * result.add(record); } return result; }
   *
   * private static final String[] getLabelNames(ResultSetMetaData rsmd, int
   * columnCount) throws SQLException { String[] result = new String[columnCount +
   * 1]; for (int i=1; i<=columnCount; i++) result[i] = rsmd.getColumnLabel(i);
   * return result; }
   */

  /*
   * backup static final List<Record> build(ResultSet rs) throws SQLException {
   * List<Record> result = new ArrayList<Record>(); ResultSetMetaData rsmd =
   * rs.getMetaData(); List<String> labelNames = getLabelNames(rsmd); while
   * (rs.next()) { Record record = new Record(); Map<String, Object> columns =
   * record.getColumns(); for (String lableName : labelNames) { Object value =
   * rs.getObject(lableName); columns.put(lableName, value); } result.add(record);
   * } return result; }
   *
   * private static final List<String> getLabelNames(ResultSetMetaData rsmd)
   * throws SQLException { int columCount = rsmd.getColumnCount(); List<String>
   * result = new ArrayList<String>(); for (int i=1; i<=columCount; i++) {
   * result.add(rsmd.getColumnLabel(i)); } return result; }
   */
}
