Structure of current Accumulo trace tables derived from org.apache.accumulo.server.TraceServer

rowId: Long.toHexString(traceId)
CF: "span"
CQ: Long.toHexString(parentSpanId) ":" Long.toHexString(spanId)
Value: bytes