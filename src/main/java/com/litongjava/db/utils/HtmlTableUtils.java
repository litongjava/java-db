package com.litongjava.db.utils;

import java.util.List;

import com.litongjava.db.activerecord.Row;
import com.litongjava.kit.RowUtils;

public class HtmlTableUtils {

  /**
   * Converts a list of Row records into an HTML table.
   *
   * @param records the list of Row objects
   * @return a String representing the HTML table, or null if records are empty
   */
  public static String to(List<Row> records) {
    // Retrieve header
    String[] head = null;
    int size = records.size();
    if (size > 0) {
      Row record = records.get(0);
      head = record.getColumnNames();
    } else {
      return null;
    }

    // Retrieve body
    List<List<Object>> body = RowUtils.getListData(records, size);

    return toHtmlTable(head, body);
  }

  /**
   * Builds an HTML table string from header and body data.
   *
   * @param head the array of column names
   * @param body the list of rows, each row is a list of cell objects
   * @return a String representing the HTML table
   */
  public static String toHtmlTable(String[] head, List<List<Object>> body) {
    StringBuilder table = new StringBuilder();

    // Start table
    table.append("<table border=\"1\">\n");

    // Add header
    table.append("  <thead>\n");
    table.append("    <tr>\n");
    for (String column : head) {
      table.append("      <th>").append(escapeHtml(column)).append("</th>\n");
    }
    table.append("    </tr>\n");
    table.append("  </thead>\n");

    // Add body
    table.append("  <tbody>\n");
    for (List<Object> row : body) {
      table.append("    <tr>\n");
      for (Object cell : row) {
        table.append("      <td>");
        if (cell != null) {
          table.append(escapeHtml(cell.toString()));
        } else {
          table.append("NULL");
        }
        table.append("</td>\n");
      }
      table.append("    </tr>\n");
    }
    table.append("  </tbody>\n");

    // End table
    table.append("</table>");

    return table.toString();
  }

  /**
   * Escapes HTML special characters in a string to prevent HTML injection.
   *
   * @param text the input string
   * @return the escaped string
   */
  private static String escapeHtml(String text) {
    if (text == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (char c : text.toCharArray()) {
      switch (c) {
      case '&':
        sb.append("&amp;");
        break;
      case '<':
        sb.append("&lt;");
        break;
      case '>':
        sb.append("&gt;");
        break;
      case '\"':
        sb.append("&quot;");
        break;
      case '\'':
        sb.append("&#39;");
        break;
      default:
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
