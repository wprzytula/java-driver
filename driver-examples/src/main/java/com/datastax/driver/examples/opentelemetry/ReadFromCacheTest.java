/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.examples.opentelemetry;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.tracing.TracingInfoFactory;
import com.datastax.driver.opentelemetry.OpenTelemetryTracingInfoFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

/**
 * Creates a keyspace and tables, and loads some data into them. Sends OpenTelemetry tracing data to
 * Zipkin tracing backend
 *
 * <p>Preconditions: - a Scylla cluster is running and accessible through the contacts points
 * identified by CONTACT_POINTS and PORT and Zipkin backend is running and accessible through the
 * contacts points identified by ZIPKIN_CONTACT_POINT and ZIPKIN_PORT.
 *
 * <p>Side effects: - creates a new keyspace "simplex" in the cluster. If a keyspace with this name
 * already exists, it will be reused; - creates two tables "simplex.songs" and "simplex.playlists".
 * If they exist already, they will be reused; - inserts a row in each table.
 */
public class ReadFromCacheTest {
  private static final String CONTACT_POINT = "127.0.0.1";
  private static final int PORT = 9042;

  private static final String ZIPKIN_CONTACT_POINT = "127.0.0.1";
  private static final int ZIPKIN_PORT = 9411;

  private Cluster cluster;
  private Session session;

  private Tracer tracer;

  public static void main(String[] args) {
    // Workaround for setting ContextStorage to ThreadLocalContextStorage.
    System.setProperty("io.opentelemetry.context.contextStorageProvider", "default");

    ReadFromCacheTest client = new ReadFromCacheTest();

    client.connect();

    try {
      client.prepare();
      client.test();
      System.out.println(
          "All requests have been completed. Now you can visit Zipkin at "
              + "http://"
              + ZIPKIN_CONTACT_POINT
              + ":"
              + ZIPKIN_PORT
              + " and examine the produced trace.");
    } finally {
      client.close();
    }
  }

  /** Initiates a connection to the cluster. */
  public void connect() {
    cluster = Cluster.builder().addContactPoints(CONTACT_POINT).withPort(PORT).build();

    System.out.println("Connected to cluster: " + cluster.getMetadata().getClusterName());

    OpenTelemetry openTelemetry =
        OpenTelemetryConfiguration.initializeForZipkin(ZIPKIN_CONTACT_POINT, ZIPKIN_PORT);
    tracer = openTelemetry.getTracerProvider().get("this");
    session = cluster.connect();
  }

  /** Creates the schema (keyspace) and table for this example. */
  public void prepare() {
    session.execute("DROP KEYSPACE IF EXISTS otel;");
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS otel WITH "
            + "replication = {'class':'SimpleStrategy', 'replication_factor':2};");
    session.execute("DROP TABLE IF EXISTS otel.test;");
    session.execute("CREATE TABLE otel.test (id int, value int, PRIMARY KEY (id));");
    BatchStatement batchStatement = new BatchStatement();
    batchStatement.add(new SimpleStatement("INSERT INTO otel.test (id, value) VALUES (4, 2);"));
    batchStatement.add(new SimpleStatement("INSERT INTO otel.test (id, value) VALUES (2, 1);"));
    batchStatement.add(new SimpleStatement("INSERT INTO otel.test (id, value) VALUES (3, 7);"));
    session.execute(batchStatement);
  }

  /** Executes queries, which are testing cache usage. */
  public void test() {
    Span parentSpan = tracer.spanBuilder("test").startSpan();
    try (Scope parentScope = parentSpan.makeCurrent()) {
      session.close();
      TracingInfoFactory tracingInfoFactory = new OpenTelemetryTracingInfoFactory(tracer);
      cluster.setTracingInfoFactory(tracingInfoFactory);
      session = cluster.connect();

      session.execute("SELECT * FROM otel.test;");
      session.execute("SELECT * FROM otel.test WHERE id = 4;");
      session.execute("SELECT * FROM otel.test BYPASS CACHE;");
      session.execute("SELECT * FROM otel.test WHERE id = 4 BYPASS CACHE;");
    } finally {
      parentSpan.end();
    }
  }

  /** Closes the session and the cluster. */
  public void close() {
    session.close();
    cluster.close();
  }
}
