import fs from "node:fs";

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

console.log("sdk-codegen-postprocess: normalized generated SDK whitespace/import drift");
