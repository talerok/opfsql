import { OPFSSyncStorage } from "../../store/backend/opfs-storage.js";
import { MemoryFileHandle } from "../../store/wal/file-handle.js";
import { ReadMarks } from "../../store/wal/read-marks.js";
import { WalStorage } from "../../store/wal/wal-storage.js";
import { Engine } from "../engine.js";

export interface Tab {
  readonly engine: Engine;
  readonly wal: WalStorage;
}

/**
 * Several engines ("tabs") on one database, coordinated the way real tabs
 * are: Web Locks for writers, read marks for snapshots. Real tabs each open
 * their own handles in readwrite-unsafe mode; opfs-mock allows one handle per
 * file, so the tabs share the handle objects instead — the bytes every tab
 * reads and writes are the same either way — and a tab that closes leaves
 * the shared main file open for the others.
 */
export async function openTabs(
  name: string,
  count: number,
  options: { busyTimeout?: number; checkpointThreshold?: number } = {},
): Promise<Tab[]> {
  const main = new OPFSSyncStorage(name, { pageSize: 4096 });
  const walFile = new MemoryFileHandle();
  const shmFile = new MemoryFileHandle();
  const lockName = `opfsql:${name}`;
  const tabs: Tab[] = [];
  for (let index = 0; index < count; index++) {
    const marks = await ReadMarks.open(shmFile, lockName);
    const wal = new WalStorage(sharedWith(main), walFile, {
      checkpointThreshold: options.checkpointThreshold ?? 8,
      marks,
    });
    const engine = await Engine.create(wal, {
      lockName,
      ...(options.busyTimeout !== undefined && {
        busyTimeout: options.busyTimeout,
      }),
    });
    tabs.push({ engine, wal });
  }
  return tabs;
}

/** `storage` for one tab: closing it leaves the file open for the others. */
function sharedWith(storage: OPFSSyncStorage): OPFSSyncStorage {
  return new Proxy(storage, {
    get(target, key) {
      if (key === "close") return keepOpen;
      const value: unknown = Reflect.get(target, key, target);
      return typeof value === "function"
        ? (value as (...parameters: unknown[]) => unknown).bind(target)
        : value;
    },
  });
}

function keepOpen(): void {
  // the other tabs still read and write the file
}
