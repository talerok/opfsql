import { it } from "vitest";
import { plan, thrown } from "./helpers.js";
const queries = process.env.Q!.split("\n;;\n");
it("prints", () => {
  const out: string[] = [];
  for (const sql of queries) {
    try {
      out.push(`${sql}\n${plan(sql)}\n`);
    } catch (error) {
      out.push(`${sql}\nERROR ${String(error)}\n`);
    }
  }
  console.warn(out.join("\n"));
});
