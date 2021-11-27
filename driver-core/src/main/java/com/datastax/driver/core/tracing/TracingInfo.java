package com.datastax.driver.core.tracing;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * An abstraction layer over instrumentation library API, corresponding to a logical span in the
 * trace.
 */
public interface TracingInfo {

  /** Final status of the traced execution. */
  enum StatusCode {
    OK,
    ERROR,
  }

  /**
   * Starts a span corresponding to this {@link TracingInfo} object. Must be called exactly once,
   * before any other method, at the beginning of the traced execution.
   *
   * @param name the name given to the span being created.
   */
  void setNameAndStartTime(String name);

  /**
   * Adds provided consistency level to the trace.
   *
   * @param consistency the consistency level to be set.
   */
  void setConsistencyLevel(ConsistencyLevel consistency);

  /**
   * Adds provided statement type to the trace.
   *
   * @param statementType the statementType to be set.
   */
  void setStatementType(String statementType);

  /**
   * Records in the trace that the provided exception occured.
   *
   * @param exception the exception to be recorded.
   */
  void recordException(Exception exception);

  /**
   * Sets the final status of the traced execution.
   *
   * @param code the status code to be set.
   */
  void setStatus(StatusCode code);

  /**
   * Sets the final status of the traced execution, with additional description.
   *
   * @param code the status code to be set.
   * @param description the additional description of the status.
   */
  void setStatus(StatusCode code, String description);

  /** Must be always called exactly once at the logical end of traced execution. */
  void tracingFinished();
}
