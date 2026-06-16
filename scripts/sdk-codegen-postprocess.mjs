import fs from "node:fs";
import path from "node:path";

function rewrite(path, transform) {
  const before = fs.readFileSync(path, "utf8");
  const after = transform(before);
  if (after !== before) {
    fs.writeFileSync(path, after, "utf8");
  }
}

rewrite("sdk/php/gen/Udb/Events/V1/EventEnvelope.php", (text) =>
  text
    .replace(/^use Google\\Protobuf\\Internal\\GPBType;\r?\n/m, "")
    .replace(/^use Google\\Protobuf\\RepeatedField;\r?\n/m, "")
    .replace(/(`payload`\.\r?\n) \*\r?\n( \* This formalizes)/u, "$1$2")
    .replace(/\r?\n+$/u, "\n"),
);

rewrite("sdk/typescript/gen/udb/events/v1/udb_events_pb.ts", (text) =>
  text
    .replace(/[ \t]+\r?\n/gu, "\n")
    .replace(/\r?\n+$/u, "\n"),
);

const GENERATED_ROOTS = [
  "sdk/php/gen",
  "sdk/go/gen",
  "sdk/typescript/gen",
  "sdk/python/gen",
  "sdk/java/gen",
  "sdk/csharp/gen",
];

const GENERATED_TEXT_EXTENSIONS = new Set([
  ".cs",
  ".go",
  ".java",
  ".php",
  ".py",
  ".pyi",
  ".ts",
]);

function walkFiles(root) {
  if (!fs.existsSync(root)) {
    return [];
  }

  const entries = fs.readdirSync(root, { withFileTypes: true });
  return entries.flatMap((entry) => {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      return walkFiles(fullPath);
    }
    if (entry.isFile()) {
      return [fullPath];
    }
    return [];
  });
}

function normalizeGeneratedText(text) {
  return text
    .replace(/[ \t]+\r?\n/gu, "\n")
    .replace(/\r\n/gu, "\n")
    .replace(/\r?\n+$/u, "\n");
}

function normalizeGoGeneratedText(text) {
  return normalizeGeneratedText(text)
    .replace(
      /(\/\/ Hybrid model:\n)\/\/   (SERVER_SIDE:[^\n]+)\n\/\/   (JWT:[^\n]+)/gu,
      "$1//\n//\t$2\n//\t$3",
    )
    .replace(/maybe ”\);/gu, "maybe '');")
    .replace(/\(WHERE email <> ”\)/gu, "(WHERE email <> '')");
}

function normalizeJavaGeneratedText(text) {
  return normalizeGeneratedText(text).replace(
    /^[ \t]*\*[ \t]*\n(?=[ \t]*\* (?:HTTP prefix:|The gateway calls|Auth method routing|The native fast-path|Signed policy bundles|Server-only control plane:))/gmu,
    "",
  );
}

function normalizePhpGeneratedText(text) {
  return fixPhpDescriptorPublicDeps(normalizeGeneratedText(text)).replace(
    /^[ \t]*\*[ \t]*\n(?=[ \t]*\* (?:Generated from protobuf message|Migration order|Hybrid model:|`GetNativeAccess`|`GetPolicyBundle`))/gmu,
    "",
  );
}

function normalizePhpDescriptorText(text) {
  return fixPhpDescriptorPublicDeps(normalizeGeneratedText(text));
}

function readVarint(bytes, offset) {
  let value = 0;
  let shift = 0;
  let cursor = offset;
  while (cursor < bytes.length) {
    const byte = bytes[cursor++];
    value += (byte & 0x7f) * 2 ** shift;
    if ((byte & 0x80) === 0) {
      return [value, cursor];
    }
    shift += 7;
  }
  throw new Error("unterminated varint in generated PHP descriptor");
}

function writeVarint(value) {
  const out = [];
  let next = value;
  do {
    let byte = next & 0x7f;
    next = Math.floor(next / 128);
    if (next !== 0) {
      byte |= 0x80;
    }
    out.push(byte);
  } while (next !== 0);
  return Buffer.from(out);
}

function skipWireValue(bytes, offset, wireType) {
  if (wireType === 0) {
    return readVarint(bytes, offset)[1];
  }
  if (wireType === 1) {
    return offset + 8;
  }
  if (wireType === 2) {
    const [length, valueOffset] = readVarint(bytes, offset);
    return valueOffset + length;
  }
  if (wireType === 5) {
    return offset + 4;
  }
  throw new Error(`unsupported wire type ${wireType} in generated PHP descriptor`);
}

function decodePhpDoubleQuotedBytes(literal) {
  const out = [];
  for (let i = 0; i < literal.length; i += 1) {
    const char = literal[i];
    if (char !== "\\") {
      out.push(char.charCodeAt(0));
      continue;
    }
    i += 1;
    const escaped = literal[i];
    if (escaped === "x") {
      out.push(Number.parseInt(literal.slice(i + 1, i + 3), 16));
      i += 2;
    } else if (/[0-7]/u.test(escaped)) {
      let octal = escaped;
      while (i + 1 < literal.length && octal.length < 3 && /[0-7]/u.test(literal[i + 1])) {
        octal += literal[++i];
      }
      out.push(Number.parseInt(octal, 8));
    } else {
      const escapes = new Map([
        ["n", 10],
        ["r", 13],
        ["t", 9],
        ["v", 11],
        ["e", 27],
        ["f", 12],
        ["\\", 92],
        ['"', 34],
        ["$", 36],
      ]);
      out.push(escapes.get(escaped) ?? escaped.charCodeAt(0));
    }
  }
  return Buffer.from(out);
}

function encodePhpDoubleQuotedBytes(bytes) {
  return Array.from(bytes, (byte) => `\\x${byte.toString(16).padStart(2, "0").toUpperCase()}`).join("");
}

function fixFileDescriptorProtoPublicDeps(fileBytes) {
  let dependencyCount = 0;
  for (let offset = 0; offset < fileBytes.length;) {
    const [tag, valueOffset] = readVarint(fileBytes, offset);
    const fieldNumber = Math.floor(tag / 8);
    const wireType = tag & 0x7;
    const next = skipWireValue(fileBytes, valueOffset, wireType);
    if (fieldNumber === 3) {
      dependencyCount += 1;
    }
    offset = next;
  }

  const chunks = [];
  let changed = false;
  for (let offset = 0; offset < fileBytes.length;) {
    const fieldStart = offset;
    const [tag, valueOffset] = readVarint(fileBytes, offset);
    const fieldNumber = Math.floor(tag / 8);
    const wireType = tag & 0x7;
    const next = skipWireValue(fileBytes, valueOffset, wireType);
    if (fieldNumber === 10 && wireType === 0) {
      const [publicDependency] = readVarint(fileBytes, valueOffset);
      if (publicDependency >= dependencyCount) {
        changed = true;
        offset = next;
        continue;
      }
    }
    chunks.push(fileBytes.subarray(fieldStart, next));
    offset = next;
  }
  return changed ? Buffer.concat(chunks) : fileBytes;
}

function fixPhpDescriptorSetPublicDeps(bytes) {
  const chunks = [];
  let changed = false;
  for (let offset = 0; offset < bytes.length;) {
    const fieldStart = offset;
    const [tag, valueOffset] = readVarint(bytes, offset);
    const fieldNumber = Math.floor(tag / 8);
    const wireType = tag & 0x7;
    const next = skipWireValue(bytes, valueOffset, wireType);
    if (fieldNumber === 1 && wireType === 2) {
      const [length, messageOffset] = readVarint(bytes, valueOffset);
      const original = bytes.subarray(messageOffset, messageOffset + length);
      const fixed = fixFileDescriptorProtoPublicDeps(original);
      if (!fixed.equals(original)) {
        changed = true;
        chunks.push(writeVarint(tag), writeVarint(fixed.length), fixed);
        offset = next;
        continue;
      }
    }
    chunks.push(bytes.subarray(fieldStart, next));
    offset = next;
  }
  return changed ? Buffer.concat(chunks) : bytes;
}

function fixPhpDescriptorPublicDeps(text) {
  return text.replace(
    /internalAddGeneratedFile\(\s*"((?:[^"\\]|\\.)*)"\s*,/gsu,
    (match, literal) => {
      const before = decodePhpDoubleQuotedBytes(literal);
      const after = fixPhpDescriptorSetPublicDeps(before);
      if (after.equals(before)) {
        return match;
      }
      return match.replace(literal, encodePhpDoubleQuotedBytes(after));
    },
  );
}

const PHP_COMMENT_DRIFT_FILES = new Set([
  path.normalize("sdk/php/gen/Udb/Core/Authn/Entity/V1/MfaPolicy.php"),
  path.normalize("sdk/php/gen/Udb/Core/Authn/Entity/V1/OTP.php"),
  path.normalize("sdk/php/gen/Udb/Core/Authn/Entity/V1/RecoveryCode.php"),
  path.normalize("sdk/php/gen/Udb/Core/Authn/Entity/V1/Session.php"),
  path.normalize("sdk/php/gen/Udb/Core/Authn/Entity/V1/User.php"),
  path.normalize("sdk/php/gen/Udb/Core/Authz/Services/V1/NativeAccessRequest.php"),
  path.normalize("sdk/php/gen/Udb/Core/Authz/Services/V1/PolicyBundleRequest.php"),
]);

for (const root of GENERATED_ROOTS) {
  for (const file of walkFiles(root)) {
    if (GENERATED_TEXT_EXTENSIONS.has(path.extname(file))) {
      const ext = path.extname(file);
      if (ext === ".go") {
        rewrite(file, normalizeGoGeneratedText);
      } else if (ext === ".java") {
        rewrite(file, normalizeJavaGeneratedText);
      } else if (ext === ".php") {
        rewrite(
          file,
          PHP_COMMENT_DRIFT_FILES.has(path.normalize(file))
            ? normalizePhpGeneratedText
            : normalizePhpDescriptorText,
        );
      } else {
        rewrite(file, normalizeGeneratedText);
      }
    }
  }
}

console.log("sdk-codegen-postprocess: normalized generated SDK whitespace/import drift");
