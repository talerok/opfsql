import type { BrowserContext, Page } from "@playwright/test";
import type { FixtureApi, Outcome, PlainResult } from "../fixture/api.js";

const DB_NAME = "e2e-test";

declare global {
  // The fixture page's API (test/e2e/fixture/main.ts).
  var opfsql: FixtureApi;
}

export async function openTab(context: BrowserContext): Promise<Page> {
  const page = await context.newPage();
  await page.goto("/");
  await page.waitForSelector('#status:has-text("ready")');
  return page;
}

export async function initDatabase(
  page: Page,
  name = DB_NAME,
  options: { busyTimeout?: number } = {},
): Promise<void> {
  await page.evaluate(
    async ([databaseName, settings]) => {
      await globalThis.opfsql.cleanOpfs(databaseName);
      await globalThis.opfsql.open(databaseName, settings);
    },
    [name, options] as const,
  );
}

export async function openDatabase(
  page: Page,
  name = DB_NAME,
  options: { busyTimeout?: number } = {},
): Promise<void> {
  await page.evaluate(
    async ([databaseName, settings]) => {
      await globalThis.opfsql.open(databaseName, settings);
    },
    [name, options] as const,
  );
}

export async function exec(
  page: Page,
  sql: string,
  parameters?: unknown[],
): Promise<PlainResult[]> {
  return page.evaluate(
    async ([text, values]) => globalThis.opfsql.exec(text, values),
    [sql, parameters] as const,
  );
}

/** Runs SQL and reports the outcome, errors included, as plain data. */
export async function attempt(
  page: Page,
  sql: string,
  parameters?: unknown[],
): Promise<Outcome> {
  return page.evaluate(
    async ([text, values]) => globalThis.opfsql.attempt(text, values),
    [sql, parameters] as const,
  );
}

/** Starts SQL in the page without waiting for it. */
export async function start(
  page: Page,
  id: string,
  sql: string,
): Promise<void> {
  await page.evaluate(
    ([key, text]) => {
      globalThis.opfsql.start(key, text);
    },
    [id, sql] as const,
  );
}

export async function settle(page: Page, id: string): Promise<Outcome> {
  return page.evaluate(async (key) => globalThis.opfsql.settle(key), id);
}

export async function disconnect(page: Page): Promise<void> {
  await page.evaluate(async () => globalThis.opfsql.disconnect());
}

export async function closeDatabase(page: Page): Promise<void> {
  await page.evaluate(async () => globalThis.opfsql.close());
}

export async function cleanOpfs(page: Page, name = DB_NAME): Promise<void> {
  await page.evaluate(
    async (databaseName) => globalThis.opfsql.cleanOpfs(databaseName),
    name,
  );
}

export async function crashTab(page: Page): Promise<void> {
  await page.close();
}

export { DB_NAME };
