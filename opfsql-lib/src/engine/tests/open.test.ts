import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, expect, it } from "vitest";
import { DatabaseInUseError } from "../../store/errors.js";
import { Engine } from "../engine.js";

// opfs-mock allows one sync access handle per file, like Firefox and WebKit,
// so it shows how Engine.open behaves where only one tab may open a database.

let seq = 0;
let dbName: string;

beforeEach(() => {
  resetMockOPFS();
  dbName = `open-${seq++}`;
});

describe("Engine.open", () => {
  it("refuses a second engine on the same database with DatabaseInUseError", async () => {
    const first = await Engine.open(dbName);

    const second = Engine.open(dbName);
    await expect(second).rejects.toThrow(DatabaseInUseError);
    await expect(second).rejects.toThrow(/open in another tab/);
    await expect(second).rejects.toHaveProperty(
      "cause.name",
      "InvalidStateError",
    );

    first.close();
  });

  it("opens again once the other engine has closed", async () => {
    const first = await Engine.open(dbName);
    await expect(Engine.open(dbName)).rejects.toThrow(DatabaseInUseError);
    first.close();

    const again = await Engine.open(dbName);
    again.close();
  });

  it("releases everything it opened when opening fails", async () => {
    (await Engine.open(dbName)).close();

    // A WAL that cannot be recovered fails the open after every file is open.
    const root = await navigator.storage.getDirectory();
    const wal = await (
      await root.getFileHandle(`${dbName}.opfsql-wal`)
    ).createSyncAccessHandle();
    wal.write(new TextEncoder().encode("not a WAL header"), { at: 0 });
    wal.close();
    await expect(Engine.open(dbName)).rejects.toThrow(/bad magic/);

    // Nothing is left holding the files: the database opens once repaired.
    await root.removeEntry(`${dbName}.opfsql-wal`);
    const engine = await Engine.open(dbName);
    engine.close();
  });
});
