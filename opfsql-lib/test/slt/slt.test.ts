import { describe, it } from 'vitest';
import { readFileSync, readdirSync } from 'fs';
import { join } from 'path';
import { Engine } from '../../src/index.js';
import { MemoryStorage } from '../../src/store/tests/memory-storage.js';
import { parseSlt, runSlt } from './runner.js';

const sltDir = new URL('.', import.meta.url).pathname;
const sltFiles = readdirSync(sltDir).filter((f) => f.endsWith('.slt'));

describe('SLT', () => {
  for (const file of sltFiles) {
    it(file, async () => {
      const source = readFileSync(join(sltDir, file), 'utf-8');
      const blocks = parseSlt(source);
      const engine = await Engine.create(new MemoryStorage());
      try {
        const result = await runSlt(engine, blocks);
        if (result.failed > 0) {
          const msgs = result.errors
            .map((e) => `Block #${e.blockIndex}: ${e.sql}\n  -> ${e.message}`)
            .join('\n\n');
          throw new Error(
            `${result.failed}/${result.total} SLT blocks failed (${result.passed} passed, ${result.skipped} skipped):\n\n${msgs}`,
          );
        }
      } finally {
        engine.close();
      }
    });
  }
});
