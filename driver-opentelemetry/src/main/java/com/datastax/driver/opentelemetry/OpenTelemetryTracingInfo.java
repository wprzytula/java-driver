package com.datastax.driver.opentelemetry;

import static com.datastax.driver.opentelemetry.PrecisionLevel.FULL;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.tracing.TracingInfo;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

public class OpenTelemetryTracingInfo implements TracingInfo {
  private Span span;
  private final Tracer tracer;
  private final Context context;
  private final PrecisionLevel precision;
  private boolean tracingStarted;

  public OpenTelemetryTracingInfo(Tracer tracer, Context context, PrecisionLevel precision) {
    this.tracer = tracer;
    this.context = context;
    this.precision = precision;
    tracingStarted = false;
  }

  public Tracer getTracer() {
    return tracer;
  }

  public Context getContext() {
    return context.with(span);
  }

  public PrecisionLevel getPrecision() {
    return precision;
  }

  private void assertStarted() {
    assert tracingStarted : "TracingInfo.setStartTime must be called before any other method";
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
    if (accuratePrecisionLevel(FULL)) {
      span.setAttribute("db.scylla.statement", statement);
    }
  }

  public void setHostname(String hostname) {
    if (accuratePrecisionLevel(FULL)) {
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

  private boolean accuratePrecisionLevel(PrecisionLevel lowestAcceptablePrecision) {
    return lowestAcceptablePrecision.comparePrecisions(precision) <= 0;
  }
}
