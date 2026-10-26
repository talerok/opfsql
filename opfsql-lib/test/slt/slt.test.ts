import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { openTestEngine } from "../../src/engine/tests/test-engine.js";
import { parseSlt, runSlt, type SltFailure } from "./runner.js";

const directory = new URL(".", import.meta.url).pathname;
const files = readdirSync(directory).filter((file) => file.endsWith(".slt"));

function describeFailure(failure: SltFailure): string {
  return `Block #${String(failure.blockIndex)}: ${failure.sql}\n  -> ${failure.message}`;
}

describe("SLT", () => {
  beforeEach(() => {
    resetMockOPFS();
  });

  for (const file of files) {
    it(file, async () => {
      const source = readFileSync(path.join(directory, file), "utf8");
      const blocks = parseSlt(source);
      const engine = await openTestEngine(`slt-${file}`);
      const session = engine.connect();
      try {
        const result = await runSlt(session, blocks);
        expect(
          result.failures.map((failure) => describeFailure(failure)),
        ).toEqual([]);
        expect(result.passed + result.skipped).toBe(result.total);
      } finally {
        await session.close();
        await engine.close();
      }
    });
  }
});
