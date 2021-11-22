package com.datastax.driver.core.tracing;

import com.datastax.driver.core.ConsistencyLevel;

public class NoopTracingInfoFactory implements TracingInfoFactory {

  private static class NoopTracingInfo implements TracingInfo {
    @Override
    public void setNameAndStartTime(String name) {}

    @Override
    public void setConsistencyLevel(ConsistencyLevel consistency) {}

    @Override
    public void setStatementType(String statementType) {}

    @Override
    public void recordException(Exception exception) {}

    @Override
    public void setStatus(StatusCode code, String description) {}

    @Override
    public void setStatus(StatusCode code) {}

    @Override
    public void tracingFinished() {}
  }

  @Override
  public TracingInfo buildTracingInfo() {
    return new NoopTracingInfo();
  }

  @Override
  public TracingInfo buildTracingInfo(TracingInfo parent) {
    return new NoopTracingInfo();
  }
}
