import { resetMockOPFS } from "opfs-mock";
import { beforeEach, expect, inject, it } from "vitest";
import { DEFAULT_PAGE_SIZE } from "../../storage/index.js";
import { openTestEngine, SMALL_PAGES } from "./test-engine.js";

beforeEach(() => {
  resetMockOPFS();
});

/** Where the database header keeps the page size: after the 12-byte prefix. */
const PAGE_SIZE_OFFSET = 12;

// Guards the test setup itself: if the "small-pages" project silently fell
// back to the default, every engine suite would stop covering page splits,
// overflow pages and checkpoints.
it("engine suites run with the page size their project declares", async () => {
  const engine = await openTestEngine("probe");
  await engine.close();
  const root = await navigator.storage.getDirectory();
  const handle = await root.getFileHandle("probe.opfsql");
  const file = await handle.getFile();
  const header = new DataView(await file.arrayBuffer());

  const expected =
    inject("pages") === "small" ? SMALL_PAGES.pageSize : DEFAULT_PAGE_SIZE;
  expect(header.getUint32(PAGE_SIZE_OFFSET)).toBe(expected);
});
