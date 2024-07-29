package com.litongjava.db.graphql;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;

public class GQL {

  private static GraphQL graphQL;

  public static void setGraphQL(GraphQL graphQL) {
    GQL.graphQL = graphQL;
  }

  public static GraphQL getGraphQL() {
    return graphQL;
  }

  public static ExecutionResult execute(String query) {
    return graphQL.execute(query);

  }

  @SuppressWarnings("deprecation")
  public static ExecutionResult execute(String query, Object context) {
    return graphQL.execute(query, context);
  }

  @SuppressWarnings("deprecation")
  public static ExecutionResult execute(String query, String operationName, Object context) {
    return graphQL.execute(query, operationName, context);
  }

  @SuppressWarnings("deprecation")
  public static ExecutionResult execute(String query, Object context, Map<String, Object> variables) {
    return graphQL.execute(query, context, variables);
  }

  @SuppressWarnings("deprecation")
  public static ExecutionResult execute(String query, String operationName, Object context,
      Map<String, Object> variables) {
    return graphQL.execute(query, operationName, context, variables);
  }

  public static ExecutionResult execute(ExecutionInput.Builder executionInputBuilder) {
    return graphQL.execute(executionInputBuilder);
  }

  public static ExecutionResult execute(UnaryOperator<ExecutionInput.Builder> builderFunction) {
    return graphQL.execute(builderFunction);
  }

  public static ExecutionResult execute(ExecutionInput executionInput) {
    return graphQL.execute(executionInput);
  }

  public static CompletableFuture<ExecutionResult> executeAsync(ExecutionInput.Builder executionInputBuilder) {
    return graphQL.executeAsync(executionInputBuilder);
  }

  public static CompletableFuture<ExecutionResult> executeAsync(UnaryOperator<ExecutionInput.Builder> builderFunction) {
    return graphQL.executeAsync(builderFunction);
  }

  public static CompletableFuture<ExecutionResult> executeAsync(ExecutionInput executionInput) {
    return graphQL.executeAsync(executionInput);
  }

}
