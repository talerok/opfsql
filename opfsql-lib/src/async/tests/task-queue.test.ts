import { describe, expect, it } from "vitest";
import { AbortError } from "../../errors.js";
import { TaskQueue } from "../task-queue.js";

// One task at a time, in the order of the calls; a task waiting for its turn
// can be cancelled, a running one finishes. A task on an idle queue starts
// at once, and that fast path must keep both promises.

const never = new AbortController().signal;

/** A task that runs until `finish` is called, noting when it starts and ends. */
function task(name: string, log: string[]) {
  const done = Promise.withResolvers<undefined>();
  return {
    run: async (): Promise<string> => {
      log.push(`${name} starts`);
      await done.promise;
      log.push(`${name} ends`);
      return name;
    },
    finish: () => {
      done.resolve(undefined);
    },
  };
}

/** A task that ends as soon as it starts, noting that it ran. */
function instant(name: string, log: string[]): () => Promise<string> {
  return async () => {
    await Promise.resolve();
    log.push(`${name} runs`);
    return name;
  };
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("TaskQueue", () => {
  it("starts a task on an idle queue at once, before run() returns", () => {
    const queue = new TaskQueue();
    const log: string[] = [];
    const first = task("a", log);
    void queue.run(first.run, never);
    expect(log).toEqual(["a starts"]);
    first.finish();
  });

  it("runs tasks queued behind a running one in the order they came", async () => {
    const queue = new TaskQueue();
    const log: string[] = [];
    const tasks = ["a", "b", "c"].map((name) => task(name, log));
    const runs = tasks.map(async (each) => queue.run(each.run, never));
    await settle();
    expect(log).toEqual(["a starts"]);
    tasks[2]!.finish();
    tasks[1]!.finish();
    await settle();
    expect(log).toEqual(["a starts"]);
    tasks[0]!.finish();
    expect(await Promise.all(runs)).toEqual(["a", "b", "c"]);
    expect(log).toEqual([
      "a starts",
      "a ends",
      "b starts",
      "b ends",
      "c starts",
      "c ends",
    ]);
  });

  it("fails a call aborted while it waits behind an idle-path call, and keeps the line", async () => {
    const queue = new TaskQueue();
    const log: string[] = [];
    const [first, second, third] = ["a", "b", "c"].map((name) =>
      task(name, log),
    );
    const cancel = new AbortController();
    const a = queue.run(first!.run, never);
    const b = queue.run(second!.run, cancel.signal);
    const c = queue.run(third!.run, never);
    cancel.abort("changed my mind");

    await expect(b).rejects.toBeInstanceOf(AbortError);
    await expect(b).rejects.toHaveProperty("cause", "changed my mind");
    expect(log).toEqual(["a starts"]);
    first!.finish();
    third!.finish();
    expect(await a).toBe("a");
    expect(await c).toBe("c");
    expect(log).toEqual(["a starts", "a ends", "c starts", "c ends"]);
  });

  it("does not run a call aborted just as its turn comes", async () => {
    const queue = new TaskQueue();
    const log: string[] = [];
    const first = task("a", log);
    const cancel = new AbortController();
    const a = queue.run(first.run, never);
    const b = queue.run(instant("b", log), cancel.signal);
    first.finish();
    await a;
    // The turn is handed over; b has not resumed yet.
    cancel.abort();
    await expect(b).rejects.toBeInstanceOf(AbortError);
    expect(log).not.toContain("b runs");
    // The queue is free again.
    expect(await queue.run(instant("after", log), never)).toBe("after");
  });

  it("refuses a call whose signal is already aborted, even on an idle queue", async () => {
    const queue = new TaskQueue();
    const cancel = new AbortController();
    cancel.abort();
    const log: string[] = [];
    await expect(
      queue.run(instant("a", log), cancel.signal),
    ).rejects.toBeInstanceOf(AbortError);
    expect(log).toEqual([]);
  });

  it("lets a running task finish when its signal aborts", async () => {
    const queue = new TaskQueue();
    const log: string[] = [];
    const first = task("a", log);
    const cancel = new AbortController();
    const a = queue.run(first.run, cancel.signal);
    await settle();
    expect(log).toEqual(["a starts"]);
    cancel.abort();
    first.finish();
    expect(await a).toBe("a");
  });

  it("goes idle once every task has finished or been cancelled", async () => {
    const queue = new TaskQueue();
    await queue.idle();
    const log: string[] = [];
    const [first, second] = ["a", "b"].map((name) => task(name, log));
    const cancel = new AbortController();
    void queue.run(first!.run, never);
    const b = queue.run(second!.run, cancel.signal);
    let isIdle = false;
    const idle = (async () => {
      await queue.idle();
      isIdle = true;
    })();
    cancel.abort();
    await expect(b).rejects.toBeInstanceOf(AbortError);
    await settle();
    expect(isIdle).toBe(false);
    first!.finish();
    await idle;
    expect(isIdle).toBe(true);
  });
});
