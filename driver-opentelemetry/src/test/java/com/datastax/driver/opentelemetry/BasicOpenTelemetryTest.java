package com.datastax.driver.opentelemetry;

import static com.datastax.driver.opentelemetry.PrecisionLevel.FULL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.core.tracing.TracingInfo;
import com.datastax.driver.core.tracing.TracingInfoFactory;
import com.datastax.driver.core.utils.CassandraVersion;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.LinkedList;
import java.util.List;
import org.testng.annotations.Test;

@CassandraVersion("3.11.11")
public class BasicOpenTelemetryTest extends CCMTestsSupport {
  private static String SERVICE_NAME = "Scylla Java driver";

  private TestTracingInfoFactory testTracingInfoFactory;

  @Override
  public void onTestContextInitialized() {
    initializeTestTracing();
    execute("CREATE TABLE t (k int PRIMARY KEY, v int)");
  }

  @Test(groups = "short")
  public void simpleFullTracingTest() {
    session().execute("USE " + keyspace);
    PreparedStatement ins = session().prepare("INSERT INTO t(k, v) VALUES (?, ?)");

    int k = 1, v = 7;
    session().execute(ins.bind(k, v));

    List<TracingInfo> spans = testTracingInfoFactory.getSpans();
    assertNotEquals(spans.size(), 0);

    TracingInfo rootSpan = spans.get(0);
    assertTrue(rootSpan instanceof TestTracingInfo);
    TestTracingInfo root = (TestTracingInfo) rootSpan;

    assertTrue(root.spanStarted);
    assertTrue(root.spanFinished);
    assertEquals(root.statusCode, TracingInfo.StatusCode.OK);
  }

  private void initializeTestTracing() {
    Resource serviceNameResource =
        Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, SERVICE_NAME));

    // Set to process the spans by the spanExporter.
    final SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .setResource(Resource.getDefault().merge(serviceNameResource))
            .build();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();

    // Add a shutdown hook to shut down the SDK.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    tracerProvider.close();
                  }
                }));

    Tracer tracer = openTelemetry.getTracerProvider().get("this");
    testTracingInfoFactory = new TestTracingInfoFactory(tracer, FULL);
    session().setTracingInfoFactory(testTracingInfoFactory);
  }

  private class TestTracingInfo implements TracingInfo {

    private final Tracer tracer;
    private final Context context;
    private final PrecisionLevel precision;

    private boolean spanStarted = false;
    private boolean spanFinished = false;
    private String spanName;
    private ConsistencyLevel consistencyLevel;
    private String statement;
    private String hostname;
    private String statementType;
    private List<Exception> exceptions;
    private StatusCode statusCode;
    private String description;

    TestTracingInfo(Tracer tracer, Context context, PrecisionLevel precision) {
      this.tracer = tracer;
      this.context = context;
      this.precision = precision;
    }

    public Tracer getTracer() {
      return tracer;
    }

    public Context getContext() {
      return context;
    }

    public PrecisionLevel getPrecision() {
      return precision;
    }

    @Override
    public void setNameAndStartTime(String name) {
      this.spanStarted = true;
      this.spanName = name;
    }

    @Override
    public void setConsistencyLevel(ConsistencyLevel consistency) {
      this.consistencyLevel = consistency;
    }

    public void setStatement(String statement) {
      if (accuratePrecisionLevel(PrecisionLevel.FULL)) {
        this.statement = statement;
      }
    }

    public void setHostname(String hostname) {
      if (accuratePrecisionLevel(PrecisionLevel.FULL)) {
        this.hostname = hostname;
      }
    }

    @Override
    public void setStatementType(String statementType) {
      this.statementType = statementType;
    }

    @Override
    public void recordException(Exception exception) {
      if (this.exceptions == null) {
        this.exceptions = new LinkedList<>();
      }
      this.exceptions.add(exception);
    }

    @Override
    public void setStatus(StatusCode code) {
      this.statusCode = code;
    }

    @Override
    public void setStatus(StatusCode code, String description) {
      this.statusCode = code;
      this.description = description;
    }

    @Override
    public void tracingFinished() {
      this.spanFinished = true;
    }

    private boolean accuratePrecisionLevel(PrecisionLevel lowestAcceptablePrecision) {
      return lowestAcceptablePrecision.comparePrecisions(precision) <= 0;
    }

    public boolean isSpanStarted() {
      return spanStarted;
    }

    public boolean isSpanFinished() {
      return spanFinished;
    }

    public String getSpanName() {
      return spanName;
    }

    public ConsistencyLevel getConsistencyLevel() {
      return consistencyLevel;
    }

    public String getStatement() {
      return statement;
    }

    public String getHostname() {
      return hostname;
    }

    public String getStatementType() {
      return statementType;
    }

    public StatusCode getStatusCode() {
      return statusCode;
    }

    public String getDescription() {
      return description;
    }
  }

  private class TestTracingInfoFactory implements TracingInfoFactory {

    private final Tracer tracer; // OpenTelemetry Tracer object
    private final PrecisionLevel precision;
    private List<TracingInfo> spans = new LinkedList<>();

    public TestTracingInfoFactory(final Tracer tracer) {
      this.tracer = tracer;
      this.precision = NORMAL;
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
}
