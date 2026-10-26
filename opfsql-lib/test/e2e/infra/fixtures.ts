import { test as base } from "@playwright/test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

export { expect } from "@playwright/test";

/**
 * Every test runs in a persistent browser profile. An ephemeral context is
 * like private browsing, which has no OPFS in WebKit; one kind of context in
 * every browser keeps the runs comparable.
 */
export const test = base.extend({
  context: async ({ playwright, browserName, baseURL }, use) => {
    const profile = await mkdtemp(path.join(tmpdir(), "opfsql-e2e-"));
    const context = await playwright[browserName].launchPersistentContext(
      profile,
      { baseURL },
    );
    await use(context);
    await context.close();
    await rm(profile, { recursive: true, force: true });
  },
});
