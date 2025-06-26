package com.litongjava.template;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.litongjava.tio.utils.hutool.ResourceUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlTemplates {

  private static final String DEFAULT_MAIN_FILE = "sql-templates/main.sql";
  private static final String DEFAULT_SQL_DIR = "sql-templates";
  private static Map<String, String> sqlTemplates = null;
  private static boolean loaded = false;

  private static final Pattern INCLUDE_PATTERN = Pattern.compile("--#include\\((.*?)\\)");

  public static synchronized void load(String mainFilePath) {
    if (loaded) {
      return;
    }
    sqlTemplates = new HashMap<>();
    parseSQLFile(mainFilePath, true);
    loaded = true;
  }

  public static synchronized void load() {
    if (loaded)
      return;
    sqlTemplates = new HashMap<>();

    URL mainFileUrl = ResourceUtil.getResource(DEFAULT_MAIN_FILE);
    if (mainFileUrl != null) {
      log.info("Loading SQL templates from main file: {}", DEFAULT_MAIN_FILE);
      parseSQLFile(DEFAULT_MAIN_FILE, true);
    } else {
      log.info("{} not found. Scanning directory: {}", DEFAULT_MAIN_FILE, DEFAULT_SQL_DIR);
      URL dirUrl = ResourceUtil.getResource(DEFAULT_SQL_DIR);
      if (dirUrl == null) {
        log.warn("SQL template directory not found: {}", DEFAULT_SQL_DIR);
        loaded = true;
        return;
      }
      List<String> listResources = ResourceUtil.listResources(DEFAULT_SQL_DIR, ".sql");
      for (String sqlFile : listResources) {
        log.info("Loading SQL templates from file: {}", sqlFile);
        // 在扫描模式下，不允许 --@ 指令，以避免混乱
        parseSQLFile(sqlFile, false);
      }
    }
    loaded = true;
  }

  /**
   * 终极修正版的 parseSQLFile 方法
   */
  private static void parseSQLFile(String filePath, boolean allowInclude) {
    URL resource = ResourceUtil.getResource(filePath);
    if (resource == null) {
      throw new RuntimeException("SQL file not found: " + filePath);
    }

    try (InputStream inputStream = resource.openStream(); BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

      String currentID = null;
      StringBuilder sqlBuilder = new StringBuilder();

      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        String trimmedLine = line.trim();

        if (trimmedLine.startsWith("--#") && !trimmedLine.startsWith("--#include")) {
          // 1. 保存上一个 SQL 块
          if (currentID != null) {
            // 只有当 sqlBuilder 有内容时才存入
            if (sqlBuilder.length() > 0) {
              sqlTemplates.put(currentID, sqlBuilder.toString());
            }
          }

          // 2. 开始一个新的 SQL 块
          String[] parts = trimmedLine.split("\\s+", 2);
          if (parts.length > 1) {
            currentID = parts[1];
            sqlBuilder = new StringBuilder(); // 重置 builder
            if (sqlTemplates.containsKey(currentID)) {
              log.warn("Duplicate SQL ID found: {}. It will be overwritten by content in file: {}", currentID, filePath);
            }
          } else {
            // 如果 --# 后面没有ID，则当前没有活动的SQL块
            currentID = null;
          }
        } else if (allowInclude && trimmedLine.startsWith("--@")) {
          // 3. 处理文件包含指令
          // 保存上一个SQL块（如果有）
          if (currentID != null && sqlBuilder.length() > 0) {
            sqlTemplates.put(currentID, sqlBuilder.toString());
          }
          // 重置状态，因为 @ 是文件级指令
          currentID = null;
          sqlBuilder = new StringBuilder();

          String[] parts = trimmedLine.split("\\s+", 2);
          if (parts.length > 1) {
            String parentPath = getParentPath(filePath);
            String includedFilePath = parentPath.isEmpty() ? parts[1] : parentPath + "/" + parts[1];
            parseSQLFile(includedFilePath, true);
          }
        } else {
          // 4. 将行内容添加到当前活动的 SQL 块
          if (currentID != null) {
            sqlBuilder.append(line).append(System.lineSeparator());
          }
        }
      }

      // 5. 保存文件末尾的最后一个 SQL 块
      if (currentID != null && sqlBuilder.length() > 0) {
        sqlTemplates.put(currentID, sqlBuilder.toString());
      }

    } catch (IOException e) {
      throw new RuntimeException("Failed to read SQL file: " + filePath, e);
    }
  }

  public static String get(String sqlId) throws IllegalArgumentException {
    if (!loaded) {
      load();
    }
    return get(sqlId, new HashSet<>());
  }

  private static String get(String sqlId, Set<String> processingIds) {
    String rawSql = sqlTemplates.get(sqlId);
    if (rawSql == null) {
      // 为了调试方便，打印出所有已加载的ID
      log.error("SQL ID not found: '{}'. Available IDs are: {}", sqlId, sqlTemplates.keySet());
      throw new IllegalArgumentException("SQL ID not found: " + sqlId);
    }

    if (!processingIds.add(sqlId)) {
      throw new IllegalStateException("Circular SQL inclusion detected: " + String.join(" -> ", processingIds) + " -> " + sqlId);
    }

    Matcher matcher = INCLUDE_PATTERN.matcher(rawSql);
    StringBuffer finalSql = new StringBuffer();

    while (matcher.find()) {
      String includedId = matcher.group(1).trim();
      String includedSql = get(includedId, processingIds);
      // 替换时，直接替换，因为被包含的SQL本身已经有换行了
      matcher.appendReplacement(finalSql, Matcher.quoteReplacement(includedSql));
    }
    matcher.appendTail(finalSql);

    processingIds.remove(sqlId);

    // 返回前去除首尾的空白，保持整洁
    return finalSql.toString().trim();
  }

  public static Map<String, String> getAll() {
    if (!loaded) {
      load();
    }
    return Collections.unmodifiableMap(sqlTemplates);
  }

  public static synchronized void reset() {
    loaded = false;
    sqlTemplates = null;
  }

  private static String getParentPath(String resourcePath) {
    int lastSlash = resourcePath.lastIndexOf('/');
    if (lastSlash == -1) {
      return "";
    }
    return resourcePath.substring(0, lastSlash);
  }
}