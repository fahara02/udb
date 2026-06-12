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
  return normalizeGeneratedText(text).replace(
    /(\/\/ Hybrid model:\n)\/\/   (SERVER_SIDE:[^\n]+)\n\/\/   (JWT:[^\n]+)/gu,
    "$1//\n//\t$2\n//\t$3",
  );
}

function normalizeJavaGeneratedText(text) {
  return normalizeGeneratedText(text).replace(
    /^ \*[ \t]*\n(?= \* (?:HTTP prefix:|The gateway calls|Auth method routing|The native fast-path|Signed policy bundles))/gmu,
    "",
  );
}

function normalizePhpGeneratedText(text) {
  return normalizeGeneratedText(text).replace(
    /^ \*[ \t]*\n(?= \* Generated from protobuf message)/gmu,
    "",
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
      } else if (ext === ".php" && PHP_COMMENT_DRIFT_FILES.has(path.normalize(file))) {
        rewrite(file, normalizePhpGeneratedText);
      } else {
        rewrite(file, normalizeGeneratedText);
      }
    }
  }
}

console.log("sdk-codegen-postprocess: normalized generated SDK whitespace/import drift");
