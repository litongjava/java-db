package com.litongjava.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoDb {

  private static MongoClient mongoClient;
  private static MongoDatabase mongoDatabase;

  public static MongoDatabase getDatabase(String databaseName) {
    // 连接到数据库
    MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
    return mongoDatabase;
  }

  public static MongoDatabase getDatabase() {
    return mongoDatabase;
  }

  public static void setClient(MongoClient mongoClient) {
    MongoDb.mongoClient = mongoClient;
  }

  public static void setDatabase(MongoDatabase mongoDatabase) {
    MongoDb.mongoDatabase = mongoDatabase;
  }
}
