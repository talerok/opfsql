import { resetMockOPFS } from "opfs-mock";
import { beforeEach, expect, inject, it } from "vitest";
import { openTestEngine } from "./test-engine.js";

beforeEach(() => {
  resetMockOPFS();
});

// Guards the test setup itself: if the "wal" project silently fell back to the
// bare backend, every engine suite would stop covering the production stack.
it("engine suites run on the backend their project declares", async () => {
  const engine = await openTestEngine("probe");
  const root = await navigator.storage.getDirectory();
  const files: string[] = [];
  for await (const [name] of root.entries()) files.push(name);
  await engine.close();

  expect(files.includes("probe.opfsql-wal")).toBe(inject("backend") === "wal");
});
