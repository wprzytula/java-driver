package com.datastax.driver.opentelemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.tracing.PrecisionLevel;
import com.datastax.driver.core.tracing.TracingInfo;
import com.datastax.driver.core.utils.CassandraVersion;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.List;
import org.testng.annotations.Test;

@CassandraVersion("3.11.11")
public class BasicOpenTelemetryNormalPrecisionTest extends CCMTestsSupport {
  private static String SERVICE_NAME = "Scylla Java driver";

  private TestTracingInfoFactory testTracingInfoFactory;

  @Override
  public void onTestContextInitialized() {
    initializeTestTracing();
    execute("CREATE TABLE t (k int PRIMARY KEY, v int)");
  }

  @Test(groups = "short")
  public void simpleInsertTracingTest() {
    session().execute("USE " + keyspace);
    PreparedStatement ins = session().prepare("INSERT INTO t(k, v) VALUES (?, ?)");

    int k = 1, v = 7;
    session().execute(ins.bind(k, v));

    List<TracingInfo> spans = testTracingInfoFactory.getSpans();
    assertNotEquals(spans.size(), 0);

    TracingInfo rootSpan = spans.get(0);
    assertTrue(rootSpan instanceof TestTracingInfo);
    TestTracingInfo root = (TestTracingInfo) rootSpan;

    assertTrue(root.isSpanStarted());
    assertTrue(root.isSpanFinished());
    assertEquals(root.getStatusCode(), TracingInfo.StatusCode.OK);
  }

  private void initializeTestTracing() {
    Resource serviceNameResource =
        Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, SERVICE_NAME));

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
    testTracingInfoFactory = new TestTracingInfoFactory(tracer, PrecisionLevel.NORMAL);
    session().setTracingInfoFactory(testTracingInfoFactory);
  }
}
