import type { ColumnBinding, LogicalAggregate } from "../../binder/types.js";
import type { IndexDef, SyncIIndexManager } from "../../store/types.js";
import type { SyncPhysicalOperator, Tuple, Value } from "../types.js";

export class PhysicalIndexMinMax implements SyncPhysicalOperator {
  private emitted = false;
  private readonly layout: ColumnBinding[];

  constructor(
    private readonly agg: LogicalAggregate,
    private readonly indexManager: SyncIIndexManager,
    private readonly hint: {
      indexDef: IndexDef;
      functionName: "MIN" | "MAX";
      keyPosition: number;
    },
  ) {
    this.layout = agg.columnBindings;
  }

  getLayout(): ColumnBinding[] {
    return this.layout;
  }

  next(): Tuple[] | null {
    if (this.emitted) return null;
    this.emitted = true;

    const result =
      this.hint.functionName === "MIN"
        ? this.indexManager.first(this.hint.indexDef.name)
        : this.indexManager.last(this.hint.indexDef.name);

    const value: Value = result ? result.key[this.hint.keyPosition] : null;
    return [[value]];
  }

  reset(): void {
    this.emitted = false;
  }
}
