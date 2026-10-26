import { describe, expect, it } from "vitest";
import { linkSignals } from "../signals.js";

describe("linkSignals", () => {
  it("links nothing for one source: the source is the signal", () => {
    const session = new AbortController();
    const linked = linkSignals([undefined, session.signal]);
    expect(linked.signal).toBe(session.signal);
    linked.dispose();
    session.abort("closed");
    expect(linked.signal.aborted).toBe(true);
  });

  it("aborts with the reason of whichever source aborts first", () => {
    const call = new AbortController();
    const session = new AbortController();
    const linked = linkSignals([call.signal, session.signal]);
    expect(linked.signal).not.toBe(call.signal);
    session.abort("closed");
    call.abort("cancelled");
    expect(linked.signal.reason).toBe("closed");
  });

  it("is aborted at once when a source already is", () => {
    const call = new AbortController();
    call.abort("cancelled");
    const linked = linkSignals([call.signal, new AbortController().signal]);
    expect(linked.signal.reason).toBe("cancelled");
  });

  it("stops following its sources once disposed", () => {
    const call = new AbortController();
    const session = new AbortController();
    const linked = linkSignals([call.signal, session.signal]);
    linked.dispose();
    call.abort("cancelled");
    expect(linked.signal.aborted).toBe(false);
  });

  it("never aborts without sources", () => {
    expect(linkSignals([undefined]).signal.aborted).toBe(false);
  });
});
