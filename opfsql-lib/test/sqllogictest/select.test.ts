import { resetMockOPFS } from "opfs-mock";
import { beforeEach, describe, it } from "vitest";
import { corpusFilesWhere, FILE_TIMEOUT, runsAsListed } from "./suite.js";

describe("sqllogictest: select1 to select3", () => {
  beforeEach(() => {
    resetMockOPFS();
  });
  const files = corpusFilesWhere((path) =>
    /^test\/select[123]\.test$/u.test(path),
  );
  for (const file of files) it(file, runsAsListed(file), FILE_TIMEOUT);
});
