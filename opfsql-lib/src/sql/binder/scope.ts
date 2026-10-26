import { SemanticError } from "../../errors.js";
import { nameKey, type Schema } from "../../schema/index.js";
import type { SelectStatement } from "../parser/index.js";
import type { logical, OutputColumn } from "../plan/index.js";

// ---------------------------------------------------------------------------
// What names see while a statement is bound. A Scope is one query's FROM:
// the relations its names find, the CTE names it sees, and the queries
// around it. A subquery in an expression is a level of its own — its outer
// row is pushed while it runs, so a column found a level out has depth 1 —
// while a query in FROM or WITH, or a compound's member, is a sibling: the
// same level, which sees the enclosing queries but not this query's FROM.
// ---------------------------------------------------------------------------

/** A query bound: its plan, and the names and types of its columns in layout order. */
export interface BoundQuery {
  readonly plan: logical.Plan;
  readonly columns: readonly OutputColumn[];
}

/** Binds a query: query.ts, reached through the context so that the files meeting subqueries need not import it. */
export type QueryBinder = (
  statement: SelectStatement,
  scope: Scope,
) => BoundQuery;

/** What every scope of one statement shares. */
export class BindContext {
  private lastIndex = -1;
  private parameters = 0;
  private readonly tablesRead = new Set<string>();

  constructor(
    readonly schema: Schema,
    readonly bindQuery: QueryBinder,
  ) {}

  /** A table index (or CTE index), unique in the statement. */
  newIndex(): number {
    this.lastIndex++;
    return this.lastIndex;
  }

  /** Records a parameter; `index` counts from 0. */
  noteParameter(index: number): void {
    this.parameters = Math.max(this.parameters, index + 1);
  }

  /** The highest parameter number used: the parameters a run needs. */
  get parameterCount(): number {
    return this.parameters;
  }

  /** Records that the statement reads table `name`, wherever it names it. */
  noteTableRead(name: string): void {
    this.tablesRead.add(nameKey(name));
  }

  /** True if what was bound so far reads table `name`. */
  hasRead(name: string): boolean {
    return this.tablesRead.has(nameKey(name));
  }
}

/** A FROM item as names see it. */
export interface Relation {
  /** What qualifies its columns: the alias or table name; null for a subquery without an alias. */
  readonly name: string | null;
  readonly tableIndex: number;
  /** Column `i` is bound as `{ tableIndex, columnIndex: i }`. */
  readonly columns: readonly OutputColumn[];
  /** Columns an unqualified name and `*` skip: the right side of USING. */
  readonly hidden: ReadonlySet<number>;
  /** Only a qualified name finds its columns: `excluded` in ON CONFLICT. */
  readonly isQualifiedOnly: boolean;
}

export function relation(
  name: string | null,
  tableIndex: number,
  columns: readonly OutputColumn[],
): Relation {
  return {
    name,
    tableIndex,
    columns,
    hidden: new Set(),
    isQualifiedOnly: false,
  };
}

/** A WITH query as the queries in its scope read it. */
export interface CteDefinition {
  readonly name: string;
  readonly cteIndex: number;
  readonly columns: readonly OutputColumn[];
  /**
   * The outermost query level its definition reads a column of, or null:
   * a query that reads it reads that level too, and is correlated to it.
   */
  readonly reach: number | null;
}

/** The CTE names a query sees, innermost first. */
interface CteNames {
  readonly definition: CteDefinition;
  readonly outer: CteNames | null;
}

/** Where a scope sits among the queries of a statement. */
interface Place {
  /** The query this one is nested in, whose FROM its names also see. */
  readonly outer: Scope | null;
  /** Subqueries in expressions between this query and the statement's. */
  readonly level: number;
  readonly ctes: CteNames | null;
  /** The scope this one was made from: where it is written. */
  readonly parent: Scope | null;
}

export class Scope {
  private readonly items: Relation[] = [];
  private outermost: number | null = null;

  private constructor(
    readonly context: BindContext,
    private readonly place: Place,
  ) {}

  static root(context: BindContext): Scope {
    const place = { outer: null, level: 0, ctes: null, parent: null };
    return new Scope(context, place);
  }

  /** The query this one is nested in, whose FROM its names also see. */
  get outer(): Scope | null {
    return this.place.outer;
  }

  /** Subqueries in expressions between this query and the statement's. */
  get level(): number {
    return this.place.level;
  }

  /** A subquery in an expression of this query: one level in. */
  subquery(): Scope {
    const { ctes } = this.place;
    const level = this.level + 1;
    return new Scope(this.context, { outer: this, level, ctes, parent: this });
  }

  /** A query in FROM or WITH, or a compound's member: the same level, without this FROM. */
  sibling(): Scope {
    return new Scope(this.context, { ...this.place, parent: this });
  }

  /** This query, seeing one more CTE name; it has no relations yet. */
  withCte(definition: CteDefinition): Scope {
    const ctes = { definition, outer: this.place.ctes };
    return new Scope(this.context, { ...this.place, ctes, parent: this });
  }

  /**
   * The outermost level a name written in this scope — in a scope made
   * from it too — read a column of, when that is outside this level.
   */
  get reach(): number | null {
    return this.outermost;
  }

  /**
   * True when a name in this level — in a nested subquery too — read a
   * column of an enclosing query, so it runs again for each of its rows.
   * Read on the scope `subquery()` returned.
   */
  get isCorrelated(): boolean {
    return this.outermost !== null;
  }

  get relations(): readonly Relation[] {
    return this.items;
  }

  /** Adds a FROM item; two of one query may not have one name. */
  add(item: Relation): void {
    const { name } = item;
    const isTaken =
      name !== null &&
      this.items.some(
        (other) => other.name !== null && nameKey(other.name) === nameKey(name),
      );
    if (isTaken) {
      throw new SemanticError(`the name ${name} is used twice in FROM`);
    }
    this.items.push(item);
  }

  /** Hides a column from unqualified names and `*`, as USING does. */
  hide(tableIndex: number, column: number): void {
    const position = this.items.findIndex(
      (item) => item.tableIndex === tableIndex,
    );
    const item = this.items[position];
    if (item === undefined) return;
    const hidden = new Set([...item.hidden, column]);
    this.items[position] = { ...item, hidden };
  }

  /** The CTE `name` stands for here, if any. */
  cte(name: string): CteDefinition | null {
    const key = nameKey(name);
    for (let names = this.place.ctes; names !== null; names = names.outer) {
      if (nameKey(names.definition.name) === key) return names.definition;
    }
    return null;
  }

  /**
   * Notes that something written here reads query level `level`, an
   * enclosing one: each scope it is written in, out to that level, depends
   * on that level's row — a subquery among them runs again for each.
   */
  noteReach(level: number): void {
    if (this.level <= level) return;
    this.outermost = Math.min(this.outermost ?? level, level);
    this.place.parent?.noteReach(level);
  }
}
