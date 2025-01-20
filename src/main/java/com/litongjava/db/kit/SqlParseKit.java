package com.litongjava.db.kit;

import java.util.regex.Pattern;

public class SqlParseKit {

  //"order\\s+by\\s+[^,\\s]+(\\s+asc|\\s+desc)?(\\s*,\\s*[^,\\s]+(\\s+asc|\\s+desc)?)*";
  //public static final String REGEX_REPLACE_ORDER_BY="order\\s+by\\s+[^,\\s]+(\\s+asc|\\s+desc)?(\\s*,\\s*[^,\\s]+(\\s+asc|\\s+desc)?)*";

  //public static final String REGEX_REPLACE_ORDER_BY = "(?i)order\\s+by\\s+[^,\\s]+(?:\\s+(?:asc|desc))?(?:\\s*,\\s*[^,\\s]+(?:\\s+(?:asc|desc))?)*";
  public static final String REGEX_REPLACE_ORDER_BY = "(?i)order\\s+by\\s+[^,]+(?:\\s+(?:asc|desc))?(?:\\s*,\\s*[^,]+(?:\\s+(?:asc|desc))?)*";

  public static final Pattern ORDER_BY_PATTERN = Pattern.compile(REGEX_REPLACE_ORDER_BY, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

  public static String replaceOrderBy(String sql) {
    return ORDER_BY_PATTERN.matcher(sql).replaceAll("");
  }

}
