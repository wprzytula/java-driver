package com.datastax.driver.opentelemetry;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.tracing.PrecisionLevel;
import com.datastax.driver.core.tracing.TracingInfo;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.util.LinkedList;
import java.util.List;

class TestTracingInfo implements TracingInfo {

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
