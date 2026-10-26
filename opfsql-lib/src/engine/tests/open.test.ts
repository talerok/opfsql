import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { DatabaseInUseError, UnsupportedFormatError } from "../../errors.js";
import { Engine } from "../engine.js";

// opfs-mock allows one sync access handle per file, like Firefox and WebKit,
// so it shows how Engine.open behaves where only one tab may open a database.

let seq = 0;
let databaseName: string;

beforeEach(() => {
  resetMockOPFS();
  databaseName = `open-${String(seq++)}`;
});

describe("Engine.open", () => {
  it("refuses a second engine on the same database with DatabaseInUseError", async () => {
    const first = await Engine.open(databaseName);

    const second = Engine.open(databaseName);
    await expect(second).rejects.toThrow(DatabaseInUseError);
    await expect(second).rejects.toThrow(/open in another tab/);
    await expect(second).rejects.toMatchObject({
      code: "DATABASE_IN_USE",
      database: databaseName,
    });
    await expect(second).rejects.toHaveProperty(
      "cause.name",
      "InvalidStateError",
    );

    await first.close();
  });

  it("opens again once the other engine has closed", async () => {
    const first = await Engine.open(databaseName);
    await expect(Engine.open(databaseName)).rejects.toThrow(DatabaseInUseError);
    await first.close();

    const again = await Engine.open(databaseName);
    await again.close();
  });

  it("releases everything it opened when opening fails", async () => {
    const first = await Engine.open(databaseName);
    await first.close();

    // A WAL that cannot be recovered fails the open after every file is open.
    const root = await navigator.storage.getDirectory();
    const walFile = await root.getFileHandle(`${databaseName}.opfsql-wal`);
    const wal = await walFile.createSyncAccessHandle();
    wal.write(new TextEncoder().encode("not a WAL header"), { at: 0 });
    wal.close();
    await expect(Engine.open(databaseName)).rejects.toThrow(
      /not an opfsql wal file/,
    );

    // Nothing is left holding the files: the database opens once repaired.
    await root.removeEntry(`${databaseName}.opfsql-wal`);
    const engine = await Engine.open(databaseName);
    await engine.close();
  });

  it("refuses a format-4 database with UNSUPPORTED_FORMAT, leaving it as it was", async () => {
    // A format-4 header: the magic, then the version as a big-endian u32.
    const header = new Uint8Array(4096);
    header.set(new TextEncoder().encode("OPFSQLDB"), 0);
    new DataView(header.buffer).setUint32(8, 4);
    const root = await navigator.storage.getDirectory();
    const file = await root.getFileHandle(`${databaseName}.opfsql`, {
      create: true,
    });
    const handle = await file.createSyncAccessHandle();
    handle.write(header, { at: 0 });
    handle.close();

    const opened = Engine.open(databaseName);
    await expect(opened).rejects.toBeInstanceOf(UnsupportedFormatError);
    await expect(opened).rejects.toMatchObject({
      code: "UNSUPPORTED_FORMAT",
      file: `${databaseName}.opfsql`,
      version: 4,
      supported: 5,
    });
    const stored = await file.getFile();
    const after = new Uint8Array(await stored.arrayBuffer());
    expect(after).toEqual(header);
  });
});
