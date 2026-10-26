import { InternalError } from "../../errors.js";
import type { PageReader } from "./page-store.js";

/**
 * What a cursor or a row holds of a tree — its pages, a position, where an
 * overflow value lies — is current only while the tree is unchanged
 * (docs/layers.md). A guard made when they were read fails once the tree has
 * changed or rolled back since, instead of letting them skip rows, repeat
 * them or read freed pages.
 */
export class TreeGuard {
  private readonly version: number;

  constructor(
    private readonly pages: PageReader,
    private readonly root: number,
  ) {
    this.version = pages.treeVersion(root);
  }

  check(): void {
    if (this.pages.treeVersion(this.root) === this.version) return;
    throw new InternalError(
      `tree ${String(this.root)} changed while what was read from it was still in use: read everything a change needs before making it`,
    );
  }
}
