/*
 * Copyright (C) 2021 ScyllaDB
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

package com.datastax.driver.opentelemetry;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.tracing.PrecisionLevel;
import com.datastax.driver.core.tracing.TracingInfo;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

public class OpenTelemetryTracingInfo implements TracingInfo {
  private Span span;
  private final Tracer tracer;
  private final Context context;
  private boolean tracingStarted;
  private final PrecisionLevel precision;

  protected OpenTelemetryTracingInfo(Tracer tracer, Context context, PrecisionLevel precision) {
    this.tracer = tracer;
    this.context = context;
    this.precision = precision;
    this.tracingStarted = false;
  }

  public Tracer getTracer() {
    return tracer;
  }

  public Context getContext() {
    return context.with(span);
  }

  private void assertStarted() {
    assert tracingStarted : "TracingInfo.setStartTime must be called before any other method";
  }

  public PrecisionLevel getPrecision() {
    return precision;
  }

  @Override
  public void setNameAndStartTime(String name) {
    assert !tracingStarted : "TracingInfo.setStartTime may only be called once.";
    tracingStarted = true;
    span = tracer.spanBuilder(name).setParent(context).startSpan();
  }

  @Override
  public void setConsistencyLevel(ConsistencyLevel consistency) {
    assertStarted();
    span.setAttribute("db.scylla.consistency_level", consistency.toString());
  }

  public void setStatement(String statement) {
    assertStarted();
    if (currentPrecisionLevelIsAtLeast(PrecisionLevel.FULL)) {
      span.setAttribute("db.scylla.statement", statement);
    }
  }

  public void setHostname(String hostname) {
    assertStarted();
    if (currentPrecisionLevelIsAtLeast(PrecisionLevel.FULL)) {
      span.setAttribute("net.peer.name", hostname);
    }
  }

  @Override
  public void setStatementType(String statementType) {
    assertStarted();
    span.setAttribute("db.scylla.statement_type", statementType);
  }

  private io.opentelemetry.api.trace.StatusCode mapStatusCode(StatusCode code) {
    switch (code) {
      case OK:
        return io.opentelemetry.api.trace.StatusCode.OK;
      case ERROR:
        return io.opentelemetry.api.trace.StatusCode.ERROR;
    }
    return null;
  }

  @Override
  public void recordException(Exception exception) {
    assertStarted();
    span.recordException(exception);
  }

  @Override
  public void setStatus(StatusCode code, String description) {
    assertStarted();
    span.setStatus(mapStatusCode(code), description);
  }

  @Override
  public void setStatus(StatusCode code) {
    assertStarted();
    span.setStatus(mapStatusCode(code));
  }

  @Override
  public void tracingFinished() {
    assertStarted();
    span.end();
  }

  private boolean currentPrecisionLevelIsAtLeast(PrecisionLevel requiredLevel) {
    return requiredLevel.compareTo(precision) <= 0;
  }
}
