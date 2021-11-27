package com.datastax.driver.core.tracing;

/** Factory of trace info objects. */
public interface TracingInfoFactory {

  /** Creates new trace info object, inheriting global context. */
  TracingInfo buildTracingInfo();

  /**
   * Creates new trace info object, inheriting context from provided another trace info object.
   *
   * @param parent the trace info object to be set as the parent of newly created trace info object.
   */
  TracingInfo buildTracingInfo(TracingInfo parent);
}
