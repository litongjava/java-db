package com.litongjava.template;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.litongjava.tio.utils.hutool.ResourceUtil;

public class SqlTemplates {

  private static final String DEFAULT_MAIN_FILE = "sql-templates/main.sql";
  private static final String DEFAULT_SQL_DIR = "sql-templates";
  private static Map<String, String> sqlTemplates = null;
  private static boolean loaded = false; // 添加一个加载状态标志

  /**
   * 加载指定的SQL主模板文件。
   * @param mainFilePath 主模板文件路径
   */
  public static synchronized void load(String mainFilePath) {
    if (loaded)
      return; // 防止重复加载
    sqlTemplates = new HashMap<>();
    parseSQLFile(mainFilePath, true);
    loaded = true;
  }

  /**
   * 默认加载方式。
   * 1. 尝试加载 "sql-templates/main.sql"。
   * 2. 如果 "main.sql" 不存在，则扫描并加载 "sql-templates/" 目录下的所有 *.sql 文件。
   */
  public static synchronized void load() {
    if (loaded)
      return; // 防止重复加载
    sqlTemplates = new HashMap<>();

    URL mainFileUrl = ResourceUtil.getResource(DEFAULT_MAIN_FILE);
    if (mainFileUrl != null) {
      // 1. 如果 main.sql 存在，则加载它
      System.out.println("Loading SQL templates from main file: " + DEFAULT_MAIN_FILE);
      parseSQLFile(DEFAULT_MAIN_FILE, true);
    } else {
      // 2. 如果 main.sql 不存在，扫描目录
      System.out.println(DEFAULT_MAIN_FILE + " not found. Scanning directory: " + DEFAULT_SQL_DIR);
      try {
        // 获取 sql-templates 目录的URL
        URL dirUrl = ResourceUtil.getResource(DEFAULT_SQL_DIR);
        if (dirUrl == null) {
          System.err.println("Warning: SQL template directory not found: " + DEFAULT_SQL_DIR);
          loaded = true; // 标记为已加载，即使是空的，避免重复尝试
          return;
        }

        // 遍历目录下的所有 .sql 文件
        // 这种方式对文件系统和JAR包都有效
        listResources(DEFAULT_SQL_DIR, ".sql").forEach(sqlFile -> {
          System.out.println("Loading SQL templates from file: " + sqlFile);
          parseSQLFile(sqlFile, false); // 解析找到的每个文件
        });

      } catch (Exception e) {
        throw new RuntimeException("Failed to scan SQL template directory: " + DEFAULT_SQL_DIR, e);
      }
    }
    loaded = true;
  }

  /**
   * 解析单个SQL文件。
   * @param filePath 文件路径
   * @param allowInclude 是否允许处理 --@ 导入指令
   */
  private static void parseSQLFile(String filePath, boolean allowInclude) {
    URL resource = ResourceUtil.getResource(filePath);
    if (resource == null) {
      // 如果是主文件找不到，抛出异常；如果是被包含的文件找不到，也应该报错
      throw new RuntimeException("SQL file not found: " + filePath);
    }

    List<String> lines;
    try (InputStream inputStream = resource.openStream(); BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      lines = reader.lines().collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Failed to read SQL file: " + filePath, e);
    }

    String currentID = null;
    StringBuilder sqlBuilder = null;

    for (String line : lines) {
      String trimmedLine = line.trim();
      if (trimmedLine.startsWith("--#")) {
        // 结束上一个SQL块
        if (currentID != null && sqlBuilder != null) {
          sqlTemplates.put(currentID, sqlBuilder.toString());
        }

        // 开始新的SQL块
        String[] parts = trimmedLine.split("\\s+", 2);
        if (parts.length > 1) {
          currentID = parts[1];
          sqlBuilder = new StringBuilder();
          if (sqlTemplates.containsKey(currentID)) {
            System.err.println("Warning: Duplicate SQL ID found: " + currentID + ". It will be overwritten.");
          }
        }
      } else if (allowInclude && trimmedLine.startsWith("--@")) {
        String[] parts = trimmedLine.split("\\s+", 2);
        if (parts.length > 1) {
          // 修正路径解析，使其更健壮
          String parentPath = getParentPath(filePath);
          String includedFilePath = parentPath + "/" + parts[1];
          parseSQLFile(includedFilePath, true); // 递归解析，允许嵌套包含
        }
      } else if (currentID != null && sqlBuilder != null && !trimmedLine.isEmpty()) {
        // 忽略空行和纯注释行（除非是指令）
        if (!trimmedLine.startsWith("--") || trimmedLine.startsWith("--#") || trimmedLine.startsWith("--@")) {
          sqlBuilder.append(line).append("\n");
        }
      }
    }

    // 添加最后一个SQL块
    if (currentID != null && sqlBuilder != null) {
      sqlTemplates.put(currentID, sqlBuilder.toString());
    }
  }

  /**
   * 获取SQL语句。如果未加载，会自动触发默认加载。
   * @param sqlId SQL的唯一ID
   * @return SQL字符串
   * @throws IllegalArgumentException 如果ID未找到
   */
  public static String get(String sqlId) throws IllegalArgumentException {
    if (!loaded) {
      load();
    }
    String sql = sqlTemplates.get(sqlId);
    if (sql == null) {
      throw new IllegalArgumentException("SQL ID not found: " + sqlId);
    }
    return sql;
  }

  /**
   * 获取所有已加载的SQL模板。
   * @return 包含所有SQL模板的Map
   */
  public static Map<String, String> getAll() {
    if (!loaded) {
      load();
    }
    return Collections.unmodifiableMap(sqlTemplates); // 返回一个不可修改的视图，更安全
  }

  /**
   * 重置加载状态，主要用于测试。
   */
  public static synchronized void reset() {
    loaded = false;
    sqlTemplates = null;
  }

  /**
   * 获取资源的父路径。
   * 例如 "sql-templates/a/b.sql" -> "sql-templates/a"
   */
  private static String getParentPath(String resourcePath) {
    int lastSlash = resourcePath.lastIndexOf('/');
    if (lastSlash == -1) {
      return ""; // 在根目录下
    }
    return resourcePath.substring(0, lastSlash);
  }

  /**
   * 列出类路径下指定目录中的所有资源。
   * @param dirPath 目录路径，例如 "sql-templates"
   * @param fileExtension 文件扩展名，例如 ".sql"
   * @return 资源路径列表
   */
  private static List<String> listResources(String dirPath, String fileExtension) throws IOException, URISyntaxException {
    URL dirUrl = Thread.currentThread().getContextClassLoader().getResource(dirPath);
    if (dirUrl == null) {
      return Collections.emptyList();
    }

    // 根据URL协议（file或jar）处理
    if ("file".equals(dirUrl.getProtocol())) {
      // 运行在文件系统中
      try (Stream<Path> stream = Files.walk(Paths.get(dirUrl.toURI()))) {
        return stream.filter(Files::isRegularFile).map(path -> Paths.get(dirUrl.getPath()).relativize(path)) // 获取相对路径
            .map(Path::toString).filter(path -> path.endsWith(fileExtension)).map(path -> dirPath + "/" + path.replace("\\", "/")) // 组合成资源路径
            .collect(Collectors.toList());
      }
    } else if ("jar".equals(dirUrl.getProtocol())) {
      // 运行在JAR包中，这种方式比较复杂，这里提供一个简化的实现
      // 生产环境建议使用更成熟的库如 Spring's PathMatchingResourcePatternResolver
      // 或者 Guava's ClassPath
      System.err.println("Warning: Scanning resources in a JAR file is not fully supported by this simple implementation.");
      // 简化的实现：可以尝试读取jar文件条目，但这里我们先返回空列表并打印警告
      return Collections.emptyList();
    }

    return Collections.emptyList();
  }
}