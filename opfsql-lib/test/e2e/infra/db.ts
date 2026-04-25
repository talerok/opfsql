import type { Page, BrowserContext } from "@playwright/test";

const DB_NAME = "e2e-test";

export async function openTab(context: BrowserContext): Promise<Page> {
  const page = await context.newPage();
  await page.goto("/");
  await page.waitForSelector('#status:has-text("ready")');
  return page;
}

export async function initDb(page: Page, dbName = DB_NAME): Promise<void> {
  await page.evaluate(async (name) => {
    await (window as any).__opfsql.cleanOpfs(name);
    await (window as any).__opfsql.open(name);
    await (window as any).__opfsql.connect();
  }, dbName);
}

export async function openDb(page: Page, dbName = DB_NAME): Promise<void> {
  await page.evaluate(async (name) => {
    await (window as any).__opfsql.open(name);
    await (window as any).__opfsql.connect();
  }, dbName);
}

export async function exec(
  page: Page,
  sql: string,
  params?: unknown[],
): Promise<any[]> {
  return page.evaluate(
    async ({ sql, params }) => {
      return (window as any).__opfsql.exec(sql, params);
    },
    { sql, params },
  );
}

export async function closeDb(page: Page): Promise<void> {
  await page.evaluate(async () => {
    await (window as any).__opfsql.close();
  });
}

export async function cleanOpfs(
  page: Page,
  dbName = DB_NAME,
): Promise<void> {
  await page.evaluate(async (name) => {
    await (window as any).__opfsql.cleanOpfs(name);
  }, dbName);
}

export async function crashTab(page: Page): Promise<void> {
  await page.close();
}

export { DB_NAME };
