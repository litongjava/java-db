package com.litongjava.jfinal.plugin.template;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.litongjava.tio.utils.hutool.ResourceUtil;

public class SqlTpls {

  private static final Map<String, String> sqlTemplates = new HashMap<>();

  public static void load(String mainFilePath) {
    parseSQLFile(mainFilePath);
  }

  private static void parseSQLFile(String filePath) {
    URL resource = ResourceUtil.getResource(filePath);
    if (resource == null) {
      throw new RuntimeException();
    }

    List<String> lines;
    try (InputStream inputStream = resource.openStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      lines = reader.lines().collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String currentID = null;

    for (String line : lines) {
      line = line.trim();
      if (line.startsWith("--#")) {
        String[] parts = line.split("\\s+");
        if (parts.length > 1) {
          currentID = parts[1];
          sqlTemplates.put(currentID, "");
        }
      } else if (line.startsWith("--@")) {
        String[] parts = line.split("\\s+");
        if (parts.length > 1) {
          String includedFilePath = Paths.get(filePath).getParent().toString() + "/" + parts[1];
          parseSQLFile(includedFilePath);
        }
      } else if (currentID != null) {
        sqlTemplates.put(currentID, sqlTemplates.get(currentID) + line + "\n");
      }
    }
  }

  public static String get(String sqlId) throws IllegalArgumentException {
    String sql = sqlTemplates.get(sqlId);
    if (sql == null) {
      throw new IllegalArgumentException("SQL ID not found");
    }
    return sql;
  }

  public static Map<String, String> getAll() {
    return sqlTemplates;
  }
}
