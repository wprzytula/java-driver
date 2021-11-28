package com.datastax.driver.opentelemetry;

import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.core.tracing.PrecisionLevel;
import com.datastax.driver.core.tracing.TracingInfo;
import com.datastax.driver.core.tracing.TracingInfoFactory;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.util.LinkedList;
import java.util.List;

class TestTracingInfoFactory implements TracingInfoFactory {

  private final Tracer tracer;
  private final PrecisionLevel precision;
  private List<TracingInfo> spans = new LinkedList<>();

  public TestTracingInfoFactory(final Tracer tracer) {
    this.tracer = tracer;
    this.precision = PrecisionLevel.NORMAL;
  }

  public TestTracingInfoFactory(final Tracer tracer, final PrecisionLevel precision) {
    this.tracer = tracer;
    this.precision = precision;
  }

  @Override
  public TracingInfo buildTracingInfo() {
    final Context current = Context.current();
    TracingInfo tracingInfo = new TestTracingInfo(tracer, current, precision);
    spans.add(tracingInfo);
    return tracingInfo;
  }

  @Override
  public TracingInfo buildTracingInfo(TracingInfo parent) {
    TracingInfo tracingInfo;

    if (parent instanceof TestTracingInfo) {
      final TestTracingInfo castedParent = (TestTracingInfo) parent;
      tracingInfo =
          new TestTracingInfo(
              castedParent.getTracer(), castedParent.getContext(), castedParent.getPrecision());
      spans.add(tracingInfo);
      return tracingInfo;
    }

    tracingInfo = new NoopTracingInfoFactory().buildTracingInfo();
    spans.add(tracingInfo);
    return tracingInfo;
  }

  public TracingInfo buildTracingInfo(Span parent) {
    final Context current = Context.current().with(parent);
    TracingInfo tracingInfo = new TestTracingInfo(tracer, current, precision);
    spans.add(tracingInfo);
    return tracingInfo;
  }

  public List<TracingInfo> getSpans() {
    return spans;
  }
}
