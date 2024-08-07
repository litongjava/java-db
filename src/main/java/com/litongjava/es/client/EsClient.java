package com.litongjava.es.client;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

public class EsClient {

  private static RestHighLevelClient client;

  public static void setClient(RestHighLevelClient client) {
    EsClient.client = client;
  }

  public static RestHighLevelClient getClient() {
    return client;
  }

  public static IndexResponse index(IndexRequest indexRequest, RequestOptions options) {
    try {
      return client.index(indexRequest, options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final Cancellable indexAsync(IndexRequest indexRequest, RequestOptions options, ActionListener<IndexResponse> listener) {
    return client.indexAsync(indexRequest, options, listener);
  }

  public static final SearchResponse search(SearchRequest searchRequest, RequestOptions options) {
    try {
      return client.search(searchRequest, options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public final Cancellable searchAsync(SearchRequest searchRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
    return client.searchAsync(searchRequest, options, listener);
  }

  public final BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) {
    try {
      return client.bulk(bulkRequest, options);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }
}
