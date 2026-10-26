import type { TupleOf } from "../../../test/support/tuple.js";
import { MemoryFile } from "../../storage/file/sync-file.js";
import type { StorageFiles } from "../../storage/index.js";
import { createEngine, type Engine } from "../engine.js";

export interface Tab {
  readonly engine: Engine;
}

export interface TabOptions {
  readonly busyTimeout?: number;
  readonly checkpointThreshold?: number;
}

/**
 * Several engines ("tabs") on one database, coordinated the way real tabs
 * are: Web Locks for writers, read marks for snapshots. Real tabs each open
 * the files in readwrite-unsafe mode; here they share memory files, whose
 * bytes every tab reads and writes the same way, and which stay open when
 * one tab closes them.
 */
export async function openTabs<N extends number>(
  name: string,
  count: N,
  options: TabOptions = {},
): Promise<TupleOf<Tab, N>> {
  const files = sharedFiles();
  const tabs: Tab[] = [];
  for (let index = 0; index < count; index++) {
    tabs.push({ engine: await openTab(name, files, options) });
  }
  // `count` of them, as the type says.
  return tabs as TupleOf<Tab, N>;
}

/** The files of one database, with a read-mark file for several tabs. */
export function sharedFiles(): StorageFiles {
  return {
    database: new MemoryFile(),
    wal: new MemoryFile(),
    marks: new MemoryFile(),
  };
}

/** One more tab on `files`, which another tab may have open. */
export async function openTab(
  name: string,
  files: StorageFiles,
  options: TabOptions = {},
): Promise<Engine> {
  return createEngine(files, {
    name,
    lockName: `opfsql:${name}`,
    pageSize: 4096,
    checkpointThreshold: options.checkpointThreshold ?? 8,
    ...(options.busyTimeout !== undefined && {
      busyTimeout: options.busyTimeout,
    }),
  });
}
