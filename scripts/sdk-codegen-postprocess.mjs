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

for (const root of GENERATED_ROOTS) {
  for (const file of walkFiles(root)) {
    if (GENERATED_TEXT_EXTENSIONS.has(path.extname(file))) {
      rewrite(file, normalizeGeneratedText);
    }
  }
}

console.log("sdk-codegen-postprocess: normalized generated SDK whitespace/import drift");
