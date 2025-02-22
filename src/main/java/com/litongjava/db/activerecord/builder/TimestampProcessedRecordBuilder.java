package com.litongjava.db.activerecord.builder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.litongjava.db.activerecord.CPI;
import com.litongjava.db.activerecord.Config;
import com.litongjava.db.activerecord.Row;
import com.litongjava.db.activerecord.RecordBuilder;

/**
 * TimestampProcessedRecordBuilder
 * 时间戳被处理过的 RecordBuilder
 * oracle 从 Connection 中取值时需要调用具体的 getTimestamp(int) 来取值
 */
public class TimestampProcessedRecordBuilder extends RecordBuilder {

  public static final TimestampProcessedRecordBuilder me = new TimestampProcessedRecordBuilder();

  @Override
  public List<Row> build(Config config, ResultSet rs) throws SQLException {
    return build(config, rs, null);
  }

  @Override
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
      CPI.setColumnsMap(record, config.getContainerFactory().getColumnsMap());
      Map<String, Object> columns = record.getColumns();
      for (int i = 1; i <= columnCount; i++) {
        Object value = BuilderKit.getColumnValue(types, rs, i);
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
}
