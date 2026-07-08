package dev.udb.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.udb.entity.v1.DeleteRequest;
import com.udb.entity.v1.MutationResponse;
import com.udb.entity.v1.RecordSet;
import com.udb.entity.v1.RequestContext;
import com.udb.entity.v1.SelectRequest;
import com.udb.entity.v1.UpsertRequest;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Bound relational entity/table facade over the canonical DataBroker RPCs. */
public final class UdbEntityHandle {
  private final UdbClient client;
  private final String messageType;
  private final List<String> key;

  UdbEntityHandle(UdbClient client, String messageType, List<String> key) {
    this.client = Objects.requireNonNull(client, "client");
    if (messageType == null || messageType.isBlank()) {
      throw new IllegalArgumentException("udb: messageType is required");
    }
    this.messageType = messageType;
    this.key = List.copyOf(key == null ? List.of() : key);
  }

  public String messageType() {
    return messageType;
  }

  public List<String> key() {
    return key;
  }

  public RecordSet select(Map<String, ?> where) {
    return select(where, 0);
  }

  public RecordSet select(Map<String, ?> where, int limit) {
    SelectRequest.Builder request =
        SelectRequest.newBuilder()
            .setContext(context(client.metadata()))
            .setMessageType(messageType)
            .setFilter(toStruct(where));
    if (limit > 0) {
      request.setLimit(limit);
    }
    return client.select(request.build());
  }

  public List<String> selectJson(Map<String, ?> where) {
    RecordSet rows = select(where);
    List<String> out = new ArrayList<>(rows.getRecordsJsonCount());
    for (ByteString json : rows.getRecordsJsonList()) {
      out.add(json.toStringUtf8());
    }
    return Collections.unmodifiableList(out);
  }

  public MutationResponse upsert(Map<String, ?> record) {
    return upsert(record, false, "");
  }

  public MutationResponse upsert(Map<String, ?> record, boolean returnRecord) {
    return upsert(record, returnRecord, "");
  }

  public MutationResponse upsert(Map<String, ?> record, boolean returnRecord, String idempotencyKey) {
    Objects.requireNonNull(record, "record");
    UpsertRequest.Builder request =
        UpsertRequest.newBuilder()
            .setContext(context(client.metadata()))
            .setMessageType(messageType)
            .setRecordJson(ByteString.copyFromUtf8(toJson(record)))
            .setPayload(toStruct(record))
            .addAllConflictFields(key)
            .setReturnRecord(returnRecord);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotencyKey(idempotencyKey);
    }
    return client.upsert(request.build());
  }

  public MutationResponse delete(Map<String, ?> where) {
    return delete(where, "");
  }

  public MutationResponse delete(Map<String, ?> where, String idempotencyKey) {
    DeleteRequest.Builder request =
        DeleteRequest.newBuilder()
            .setContext(context(client.metadata()))
            .setMessageType(messageType)
            .setFilter(toStruct(where));
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotencyKey(idempotencyKey);
    }
    return client.delete(request.build());
  }

  private static RequestContext context(UdbMetadata meta) {
    return RequestContext.newBuilder()
        .setTenantId(meta.tenantId())
        .setUserId(meta.userId())
        .setCorrelationId(meta.correlationId())
        .setPurpose(meta.purpose())
        .addAllScopes(meta.scopes())
        .setServiceIdentity(meta.serviceIdentity())
        .setProjectId(meta.projectId())
        .setClientCatalogVersion(meta.clientCatalogVersion())
        .setConsistency(meta.consistency())
        .setPrimaryRead(meta.primaryRead())
        .setMaxReplicaLagMs(meta.maxReplicaLagMs())
        .setEventualConsistencyAllowed(meta.eventualConsistencyAllowed())
        .setReadFenceJson(meta.readFenceJson())
        .build();
  }

  private static Struct toStruct(Map<String, ?> values) {
    Struct.Builder builder = Struct.newBuilder();
    if (values == null) {
      return builder.build();
    }
    for (Map.Entry<String, ?> entry : values.entrySet()) {
      builder.putFields(entry.getKey(), toValue(entry.getValue()));
    }
    return builder.build();
  }

  private static Value toValue(Object value) {
    if (value == null) {
      return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    }
    if (value instanceof Boolean bool) {
      return Value.newBuilder().setBoolValue(bool).build();
    }
    if (value instanceof Number number) {
      double numeric = number.doubleValue();
      if (!Double.isFinite(numeric)) {
        throw new IllegalArgumentException("udb: protobuf Struct does not support non-finite numbers");
      }
      return Value.newBuilder().setNumberValue(numeric).build();
    }
    if (value instanceof Map<?, ?> map) {
      Struct.Builder nested = Struct.newBuilder();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        nested.putFields(String.valueOf(entry.getKey()), toValue(entry.getValue()));
      }
      return Value.newBuilder().setStructValue(nested).build();
    }
    if (value instanceof Iterable<?> items) {
      ListValue.Builder list = ListValue.newBuilder();
      for (Object item : items) {
        list.addValues(toValue(item));
      }
      return Value.newBuilder().setListValue(list).build();
    }
    if (value.getClass().isArray()) {
      ListValue.Builder list = ListValue.newBuilder();
      int len = Array.getLength(value);
      for (int idx = 0; idx < len; idx++) {
        list.addValues(toValue(Array.get(value, idx)));
      }
      return Value.newBuilder().setListValue(list).build();
    }
    return Value.newBuilder().setStringValue(String.valueOf(value)).build();
  }

  private static String toJson(Object value) {
    StringBuilder out = new StringBuilder();
    writeJson(out, value);
    return out.toString();
  }

  @SuppressWarnings("unchecked")
  private static void writeJson(StringBuilder out, Object value) {
    if (value == null) {
      out.append("null");
    } else if (value instanceof Boolean || value instanceof Byte || value instanceof Short
        || value instanceof Integer || value instanceof Long) {
      out.append(value);
    } else if (value instanceof Float || value instanceof Double) {
      double numeric = ((Number) value).doubleValue();
      if (!Double.isFinite(numeric)) {
        throw new IllegalArgumentException("udb: JSON does not support non-finite numbers");
      }
      out.append(value);
    } else if (value instanceof Number) {
      out.append(value);
    } else if (value instanceof Map<?, ?> map) {
      TreeMap<String, Object> sorted = new TreeMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        sorted.put(String.valueOf(entry.getKey()), entry.getValue());
      }
      out.append('{');
      boolean first = true;
      for (Map.Entry<String, Object> entry : sorted.entrySet()) {
        if (!first) {
          out.append(',');
        }
        first = false;
        writeJsonString(out, entry.getKey());
        out.append(':');
        writeJson(out, entry.getValue());
      }
      out.append('}');
    } else if (value instanceof Iterable<?> items) {
      out.append('[');
      boolean first = true;
      for (Object item : items) {
        if (!first) {
          out.append(',');
        }
        first = false;
        writeJson(out, item);
      }
      out.append(']');
    } else if (value.getClass().isArray()) {
      out.append('[');
      int len = Array.getLength(value);
      for (int idx = 0; idx < len; idx++) {
        if (idx > 0) {
          out.append(',');
        }
        writeJson(out, Array.get(value, idx));
      }
      out.append(']');
    } else {
      writeJsonString(out, String.valueOf(value));
    }
  }

  private static void writeJsonString(StringBuilder out, String value) {
    out.append('"');
    for (int idx = 0; idx < value.length(); idx++) {
      char ch = value.charAt(idx);
      switch (ch) {
        case '"' -> out.append("\\\"");
        case '\\' -> out.append("\\\\");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (ch < 0x20) {
            out.append(String.format("\\u%04x", (int) ch));
          } else {
            out.append(ch);
          }
        }
      }
    }
    out.append('"');
  }
}
