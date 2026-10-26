# Tabs sharing a database

How tabs read and write one database at once, what each may change, and why
no tab ever reads a page of the wrong snapshot. `docs/file-format.md` has the
byte layouts this refers to; `src/storage/pager` implements it.

Several tabs open the same files only in Chromium (`readwrite-unsafe`
handles). Elsewhere a second tab gets `DATABASE_IN_USE`; the one tab still
keeps the read-mark file, and everything below holds trivially for it.

## Shared state

Tabs share no memory. All they share is three files and some Web Locks.

| State        | Where                       | Meaning                                                                                        |
| ------------ | --------------------------- | ---------------------------------------------------------------------------------------------- |
| commits      | WAL frames                  | the transactions `B+1 … L` of the current generation, each its page frames and a commit record |
| `E`, `B`     | WAL header                  | the generation (epoch), and its base: the last transaction before it                           |
| `L`          | WAL frames                  | the latest commit: the last whose commit record is complete                                    |
| file content | database file               | between checkpoints, every page as of some commit `F`, the file level: `B ≤ F ≤ L`             |
| `I`          | read-mark file, intent      | an upper bound of the file level: the database file may hold commits up to `I`, never past it  |
| marks        | read-mark file, one per tab | the oldest snapshot the tab has pinned, or none                                                |
| write lock   | Web Lock `<name>:write`     | one writer at a time, among every tab                                                          |
| open lock    | Web Lock `<name>:open`      | one tab at a time creates or recovers the files                                                |
| slot locks   | Web Lock `<name>:slot:<i>`  | a tab owns mark slot `i` while it holds this; the browser releases it when the tab dies        |

`F` is not written anywhere. It is what the database file holds, and only
the invariants below say where it may be.

Each tab also keeps what it has read of the WAL: its index of page versions,
its latest commit, the epoch it is in. That state is a cache of the files.
It decides nothing on its own: every operation that changes shared state
first reads the WAL up to its end ("catches up").

## Invariants

1. **The file never runs ahead of the intent.** `F ≤ I` at every moment.
   The intent only grows, except when a stale one is reset (rule 7).
2. **A snapshot is pinned only at or above the intent.** A tab pins `S`
   only if it read `I ≤ S` after publishing a mark `≤ S`.
3. **The file never passes a mark.** No checkpoint raises `F` above any
   published mark, whether or not the snapshot behind it will be pinned.
4. **A pinned snapshot never changes.** It is its commit `S`, its page
   count at the moment it was pinned, and the versions up to `S` in the
   tab's index. A page with a version `≤ S` in the index is read there;
   any other page is read from the database file, whose copy of it is the
   same at every commit from `F` to `S` (by 1–3, `F ≤ S` while it is
   pinned, and every change after `F` is in the WAL).
5. **Only the write-lock holder changes the WAL, the database file or the
   intent.** It appends commits, checkpoints, restarts the WAL and resets a
   stale intent, and catches up before each, so it decides on `L` itself,
   not on what it last read. Any tab changes only its own mark.
6. **The WAL restarts only when the file holds all of it.** Under the write
   lock, after catching up, and only if `F = L`. Then no snapshot below `L`
   is pinned anywhere (by 3), and the new generation's base is `L`.
7. **An intent no commit backs is stale.** `I > L` happens only when the
   database and WAL files were removed and the read-mark file kept. The
   write-lock holder, caught up, lowers it to `L`.
8. **Every write to a shared file leaves it valid at every step.** See
   [Writes another tab may see half done](#writes-another-tab-may-see-half-done).

## Operations

### Catching up

Read the WAL header, then the frames past what the tab has read, then the
header again. A frame that is torn, of another epoch or out of sequence ends
the read, and a transaction counts only once its commit record is read. If
the second header's epoch differs from the first, a restart happened
meanwhile: drop the index and read the new generation from its start, the
same way. Only an unchanged epoch makes what was read one generation,
because a restart writes its new header before it cuts the old frames off.
Reading the size of the file alone would not do: a restart can shorten the
WAL between the header and the frames, and the tab would take its
generation's commits for none.

### Pinning a snapshot (reading)

1. Catch up.
2. Publish the tab's oldest pinned snapshot, now including `S = L`, as its
   mark.
3. Read `I`. If `I ≤ S`, `S` is pinned. If not, a checkpoint has passed `S`:
   unpublish it, catch up, and try again while catching up finds commits.
4. If catching up finds nothing and `I > S` still, the intent is stale
   (rule 7): the engine takes the write lock, resets it, and tries again.

The snapshot remembers its page count when it is pinned, and keeps every
version up to `S` in its tab's index until it is released.

### Writing

Under the write lock: catch up, reset a stale intent, pin `L`, change pages
in a private copy, then append them and a commit record as one write and
flush. The flush is the commit point. The commit checks that `L` has not
moved, which the lock already guarantees.

### Checkpointing

Under the write lock, after catching up:

1. Raise the intent to `L` (if it is below): `I := max(I, L)`.
2. Then read every mark.
3. Copy into the database file the newest version up to
   `T = min(L, oldest mark)` of every page the WAL changed, and flush. `F`
   is now `T`, or stays higher if another tab copied further.
4. If `T = L`, restart the WAL.

The intent is written before the marks are read, and a reader publishes its
mark before reading the intent, so of a reader and a checkpoint that race,
at least one sees the other: either the checkpoint sees the mark and copies
no further than it, or the reader sees an intent above its snapshot and
pins a newer one (rules 2 and 3). Raising the intent above what is then
copied is harmless: readers only need `I ≥ F`. Lowering it would not be: a
reader that published an old mark and then read the lowered intent would
pin a snapshot the file had already passed.

### Restarting the WAL

Under the write lock, when `F = L`:

1. Truncate the database file's free tail: pages free at `L` past the last
   one in use. No snapshot reads them; every pinned snapshot is `L`.
2. Write the new header — epoch `E + 1`, base `L`, checksum — and flush.
3. Truncate the WAL to the header.

A tab that reads the new header drops its index: everything up to `L` is
in the database file. A snapshot it pinned at `L` before reads its pages
from the file from then on, and they are the same.

### Resetting a stale intent

Under the write lock, after catching up: if `I > L`, set `I := L`.

### Opening

Under the open lock: create the database file if it is empty, open or
create the WAL, and recover it by reading its frames — reading only.
Then claim a free mark slot by taking its lock, clear the slot's old mark,
and clear the marks of slots whose lock is free (tabs that died).

### A tab dying

At any point. Its slot lock goes with it, so its mark is cleared by the
next tab that finds the lock free. A half-written WAL append is a torn tail:
its frames fail their checksums and are ignored, and the transaction never
committed. A half-done checkpoint left pages written that no pinned snapshot
reads from the file (rule 4), and an intent at least as high as them; the
next checkpoint copies them again. A half-done restart left either the old
header, so the next checkpoint restarts again, or the new one, under which
the old frames' epoch rejects them.

## Writes another tab may see half done

| Write                 | What a reader may meet                              | Why it is still valid                                                                        |
| --------------------- | --------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| a WAL append          | some of a transaction's frames                      | each frame has a checksum; a transaction counts only once its commit record is read          |
| the WAL header        | old and new bytes mixed                             | a checksum: a reader reads it again, and only one that keeps failing is corrupt              |
| a WAL restart         | the new header over old frames, before the truncate | the old frames carry the old epoch; a reader reads the header before and after the frames    |
| a checkpoint's copies | pages of the database file being rewritten          | no pinned snapshot reads those pages from the file: their versions are in its index (rule 4) |
| a mark, the intent    | —                                                   | one aligned 8-byte write, read whole (see Assumptions)                                       |

## Assumptions about the platform

- Writes and reads of one file through different handles are seen in the
  order they happen: a read after a write returns it.
- An aligned 8-byte write is read whole, never half old and half new. Marks
  and the intent rely on it. (For the small integers they hold, the two
  halves of a double differ only in its first four bytes, so even a torn
  write reads as the old value or the new one until transaction ids pass
  2^21.)
- A Web Lock held by a tab is released when the tab dies.

## How it is checked

- Unit tests for each rule, including each operation done by a tab that has
  not caught up (`src/storage/pager/tests`).
- An interleaving test: tabs over shared memory files, where at every file
  operation of one tab another may run a whole step first — read, hold a
  snapshot, write, checkpoint — chosen by a seeded generator, with and
  without writes torn in two. It checks every read against a model of what
  each commit wrote, that every page of a held snapshot stays byte for byte
  the same, and that the intent never falls; a failure names its seed.
- End-to-end tests in real browsers, tabs included (`test/e2e`).
