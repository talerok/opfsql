import { describe, expect, it } from "vitest";
import { InternalError } from "../../errors.js";
import { NO_PARAMETERS } from "../../values/index.js";
import { createRun } from "../index.js";

describe("OuterRows", () => {
  it("sets aside the rows above a level while an action runs, and restores them", () => {
    const { outer } = createRun(NO_PARAMETERS);
    const seen = outer.within(["level 0 row"], () =>
      outer.within(["level 1 row"], () => {
        const atLevel1 = outer.atLevel(1, () => outer.at(1));
        expect(() => outer.atLevel(1, () => outer.at(2))).toThrow(
          InternalError,
        );
        return [atLevel1, outer.at(1), outer.at(2)];
      }),
    );
    expect(seen).toEqual([["level 0 row"], ["level 1 row"], ["level 0 row"]]);
  });
});

describe("createRun", () => {
  it("gives a checkpoint that does nothing unless the engine passes one", () => {
    expect(() => {
      createRun(NO_PARAMETERS).checkpoint();
    }).not.toThrow();
    let checkpoints = 0;
    createRun(NO_PARAMETERS, () => {
      checkpoints++;
    }).checkpoint();
    expect(checkpoints).toBe(1);
  });
});
