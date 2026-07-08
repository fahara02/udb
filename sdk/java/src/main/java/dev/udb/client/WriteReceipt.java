package dev.udb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Typed durability receipt returned by a mutation via
 * {@code MutationResponse.write_receipt_json}. Mirrors the Rust
 * {@code WriteReceipt} serde shape: all five fields are always present.
 */
public record WriteReceipt(
    String sourceLsn,
    long outboxSeq,
    List<String> projectionTaskIds,
    String manifestChecksum,
    long writtenAtUnixMs) {
  private static final Pattern STRING_FIELD =
      Pattern.compile("\"%s\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
  private static final Pattern NUMBER_FIELD = Pattern.compile("\"%s\"\\s*:\\s*(\\d+)");
  private static final Pattern STRING_ARRAY_FIELD =
      Pattern.compile("\"%s\"\\s*:\\s*\\[(.*?)\\]", Pattern.DOTALL);
  private static final Pattern ARRAY_STRING = Pattern.compile("\"((?:\\\\.|[^\"])*)\"");

  public WriteReceipt {
    sourceLsn = sourceLsn == null ? "" : sourceLsn;
    projectionTaskIds =
        projectionTaskIds == null ? List.of() : List.copyOf(projectionTaskIds);
    manifestChecksum = manifestChecksum == null ? "" : manifestChecksum;
  }

  public WriteReceipt() {
    this("", 0, List.of(), "", 0);
  }

  public static WriteReceipt fromJson(String json) {
    if (json == null || json.isBlank()) {
      return new WriteReceipt();
    }
    return new WriteReceipt(
        stringField(json, "source_lsn"),
        numberField(json, "outbox_seq"),
        stringArrayField(json, "projection_task_ids"),
        stringField(json, "manifest_checksum"),
        numberField(json, "written_at_unix_ms"));
  }

  public boolean isEmpty() {
    return sourceLsn.isEmpty()
        && outboxSeq == 0
        && projectionTaskIds.isEmpty()
        && manifestChecksum.isEmpty()
        && writtenAtUnixMs == 0;
  }

  public String toJson() {
    return "{"
        + "\"manifest_checksum\":\"" + escape(manifestChecksum) + "\","
        + "\"outbox_seq\":" + outboxSeq + ","
        + "\"projection_task_ids\":" + stringArrayJson(projectionTaskIds) + ","
        + "\"source_lsn\":\"" + escape(sourceLsn) + "\","
        + "\"written_at_unix_ms\":" + writtenAtUnixMs
        + "}";
  }

  static String escape(String value) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      switch (ch) {
        case '\\' -> out.append("\\\\");
        case '"' -> out.append("\\\"");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> out.append(ch);
      }
    }
    return out.toString();
  }

  private static String unescape(String value) {
    StringBuilder out = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (ch != '\\' || i + 1 >= value.length()) {
        out.append(ch);
        continue;
      }
      char escaped = value.charAt(++i);
      switch (escaped) {
        case '"' -> out.append('"');
        case '\\' -> out.append('\\');
        case '/' -> out.append('/');
        case 'b' -> out.append('\b');
        case 'f' -> out.append('\f');
        case 'n' -> out.append('\n');
        case 'r' -> out.append('\r');
        case 't' -> out.append('\t');
        default -> out.append(escaped);
      }
    }
    return out.toString();
  }

  private static String stringField(String json, String field) {
    Matcher matcher = Pattern.compile(STRING_FIELD.pattern().formatted(field)).matcher(json);
    return matcher.find() ? unescape(matcher.group(1)) : "";
  }

  private static long numberField(String json, String field) {
    Matcher matcher = Pattern.compile(NUMBER_FIELD.pattern().formatted(field)).matcher(json);
    return matcher.find() ? Long.parseLong(matcher.group(1)) : 0;
  }

  private static List<String> stringArrayField(String json, String field) {
    Matcher fieldMatcher =
        Pattern.compile(STRING_ARRAY_FIELD.pattern().formatted(field), Pattern.DOTALL).matcher(json);
    if (!fieldMatcher.find()) {
      return List.of();
    }
    List<String> values = new ArrayList<>();
    Matcher valueMatcher = ARRAY_STRING.matcher(fieldMatcher.group(1));
    while (valueMatcher.find()) {
      values.add(unescape(valueMatcher.group(1)));
    }
    return values;
  }

  static String stringArrayJson(List<String> values) {
    StringBuilder out = new StringBuilder("[");
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        out.append(',');
      }
      out.append('"').append(escape(values.get(i))).append('"');
    }
    return out.append(']').toString();
  }
}
