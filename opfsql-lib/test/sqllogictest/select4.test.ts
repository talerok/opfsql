import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, it } from "vitest";
import { corpusFilesWhere, FILE_TIMEOUT, runsAsListed } from "./suite.js";

describe("sqllogictest: select4", () => {
  beforeEach(() => {
    resetMockOPFS();
  });
  const files = corpusFilesWhere((path) => path === "test/select4.test");
  for (const file of files) it(file, runsAsListed(file), FILE_TIMEOUT);
});
