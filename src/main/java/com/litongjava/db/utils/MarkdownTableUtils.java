package com.litongjava.db.utils;

import java.util.List;
import java.util.Map;

import com.litongjava.db.activerecord.Row;
import com.litongjava.kit.RowUtils;

public class MarkdownTableUtils {

  public static String to(List<Row> records) {
    // 获取head
    String[] head = null;
    int size = records.size();
    if (size > 0) {
      Row record = records.get(0);
      head = record.getColumnNames();
    } else {
      return null;
    }

    // 获取body
    List<List<Object>> body = RowUtils.getListData(records, size);

    return toMarkdownTable(head, body);
  }

  public static String toMarkdownTable(String[] head, List<List<Object>> body) {
    StringBuilder table = new StringBuilder();

    // Add header
    table.append("| ");
    for (String column : head) {
      table.append(column).append(" | ");
    }
    table.append("\n");

    // Add separator
    table.append("| ");
    for (int i = 0; i < head.length; i++) {
      table.append("--- | ");
    }
    table.append("\n");

    // Add rows
    for (List<Object> row : body) {
      table.append("| ");
      for (Object cell : row) {
        if (cell != null) {
          table.append(cell.toString()).append(" | ");
        } else {
          table.append("NULL").append(" | ");
        }
      }
      table.append("\n");
    }

    return table.toString();
  }

  /**
   * 将单个 Row 对象转换为 Markdown 格式的键值列表
   *
   * 输出示例：
   * - key1: value1
   * - key2: value2
   *
   * @param row 要转换的 Row 对象
   * @return Markdown 格式的键值列表字符串
   */
  public static String toItems(Row row) {
    if (row == null) {
      return "";
    }

    StringBuilder markdownList = new StringBuilder();

    // 获取所有列名和对应的值
    String[] columns = row.getColumnNames();
    Map<String, Object> dataMap = row.toMap();

    for (String column : columns) {
      Object value = dataMap.get(column);
      if (value != null) {
        markdownList.append("- ").append(column).append(": ").append(value.toString()).append("\n");
      } else {
        markdownList.append("- ").append(column).append(": NULL\n");
      }
    }

    return markdownList.toString();
  }

}
