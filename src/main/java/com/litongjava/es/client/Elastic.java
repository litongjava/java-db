package com.litongjava.es.client;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

public class Elastic {

  private static RestHighLevelClient client;

  public static void setClient(RestHighLevelClient client) {
    Elastic.client = client;
  }

  public static RestHighLevelClient getClient() {
    return client;
  }

  public static boolean ping(RequestOptions options) {
    try {
      return client.ping(options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  public static Cancellable searchAsync(SearchRequest searchRequest, RequestOptions options, ActionListener<SearchResponse> listener) {
    return client.searchAsync(searchRequest, options, listener);
  }

  public static BulkResponse bulk(BulkRequest bulkRequest, RequestOptions options) {
    try {
      return client.bulk(bulkRequest, options);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  public static GetResponse get(GetRequest getRequest, RequestOptions options) {
    try {
      return client.get(getRequest, options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Cancellable getAsync(GetRequest getRequest, RequestOptions options, ActionListener<GetResponse> listener) {
    return client.getAsync(getRequest, options, listener);
  }

  public static IndicesClient indices() {
    return client.indices();
  }

  @SuppressWarnings("deprecation")
  public static CreateIndexResponse crateIndex(org.elasticsearch.action.admin.indices.create.CreateIndexRequest createIndexRequest,
      RequestOptions options) {
    try {
      return client.indices().create(createIndexRequest, options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("deprecation")
  public static Cancellable createIndexAsync(org.elasticsearch.action.admin.indices.create.CreateIndexRequest createIndexRequest,
      RequestOptions options, ActionListener<org.elasticsearch.action.admin.indices.create.CreateIndexResponse> listener) {
    return client.indices().createAsync(createIndexRequest, options, listener);
  }

  @SuppressWarnings("deprecation")
  public static boolean existsIndex(org.elasticsearch.action.admin.indices.get.GetIndexRequest request, RequestOptions options) {
    try {
      return client.indices().exists(request, options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("deprecation")
  public static Cancellable existsIndexAsync(org.elasticsearch.action.admin.indices.get.GetIndexRequest request, RequestOptions options,
      ActionListener<Boolean> listener) {
    return client.indices().existsAsync(request, options, listener);
  }

  public static UpdateResponse update(UpdateRequest updateRequest, RequestOptions options) {
    try {
      return client.update(updateRequest, options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Cancellable updateAsync(UpdateRequest updateRequest, RequestOptions options, ActionListener<UpdateResponse> listener) {
    return client.updateAsync(updateRequest, options, listener);
  }

  public static DeleteResponse delete(DeleteRequest deleteRequest, RequestOptions options) {
    try {
      return client.delete(deleteRequest, options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Cancellable delete(DeleteRequest deleteRequest, RequestOptions options, ActionListener<DeleteResponse> listener) {
    return client.deleteAsync(deleteRequest, options, listener);
  }
}
