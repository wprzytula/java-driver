package com.datastax.driver.examples.opentelemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

/** Example showing how to configure OpenTelemetry for tracing with Scylla Java Driver. */
class OpenTelemetryConfiguration {
  private static final String SERVICE_NAME = "Scylla Java driver";

  public static OpenTelemetry initialize(SpanExporter spanExporter) {
    Resource serviceNameResource =
        Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, SERVICE_NAME));

    // Set to process the spans by the spanExporter.
    final SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
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

    // Return the configured instance so it can be used for instrumentation.
    return openTelemetry;
  }

  public static OpenTelemetry initializeForZipkin(String ip, int port) {
    String endpointPath = "/api/v2/spans";
    String httpUrl = String.format("http://%s:%s", ip, port);

    SpanExporter exporter =
        ZipkinSpanExporter.builder().setEndpoint(httpUrl + endpointPath).build();

    return initialize(exporter);
  }
}
