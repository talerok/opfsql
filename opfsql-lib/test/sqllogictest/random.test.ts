import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, it } from "vitest";
import { corpusFilesWhere, FILE_TIMEOUT, runsAsListed } from "./suite.js";

describe("sqllogictest: random", () => {
  beforeEach(() => {
    resetMockOPFS();
  });
  const files = corpusFilesWhere((path) => path.startsWith("test/random/"));
  for (const file of files) it(file, runsAsListed(file), FILE_TIMEOUT);
});
