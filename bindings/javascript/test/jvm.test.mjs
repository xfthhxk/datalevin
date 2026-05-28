import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { __testing, jvmStarted, resolveClasspath } from "../src/jvm.js";

test("findRuntimeJars prefers shared runtime naming and ignores unrelated jars", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "dtlv-js-jars-"));

  try {
    const sharedJar = path.join(dir, "datalevin-runtime-0.10.15.jar");
    const legacyJar = path.join(dir, "datalevin-python-runtime-0.10.15.jar");
    const javaJar = path.join(dir, "datalevin-java-0.10.15.jar");
    fs.writeFileSync(sharedJar, "");
    fs.writeFileSync(legacyJar, "");
    fs.writeFileSync(javaJar, "");
    fs.writeFileSync(path.join(dir, "other.jar"), "");

    assert.deepEqual(__testing.findRuntimeJars(dir), [javaJar, legacyJar, sharedJar].sort());
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test("splitShellWords handles quoted JVM args", () => {
  assert.deepEqual(
    __testing.splitShellWords('-Xmx1g "-Dfoo=bar baz" -Dabc=123'),
    ["-Xmx1g", "-Dfoo=bar baz", "-Dabc=123"]
  );
});

test("resolveJvmArgs injects Datalevin defaults", () => {
  const args = __testing.resolveJvmArgs(["-Xmx1g"]);

  assert.equal(args[0], "-Xmx1g");
  assert.ok(args.includes("--enable-native-access=ALL-UNNAMED"));
  assert.ok(args.includes("--add-opens=java.base/java.lang=ALL-UNNAMED"));
  assert.ok(args.includes("--add-opens=java.base/java.util=ALL-UNNAMED"));
  assert.ok(args.includes("--add-opens=java.base/java.nio=ALL-UNNAMED"));
  assert.ok(args.includes("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"));
  assert.ok(args.some((arg) => arg.startsWith("-Dorg.bytedeco.javacpp.cachedir=")));
});

test("resolveClasspath normalizes explicit classpath entries", () => {
  assert.deepEqual(resolveClasspath(["./target/example.jar"]), [path.resolve("./target/example.jar")]);
});

test("jvmStarted is false before the bridge is initialized", () => {
  assert.equal(jvmStarted(), false);
});
