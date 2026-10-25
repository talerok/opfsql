import type {
  ColumnConstraint,
  ColumnDefinition,
  ParsedExpression,
} from "../../../../sql/parser/types.js";

/** What a column definition's constraints, as written, add up to. */
export interface ColumnConstraints {
  /** PRIMARY KEY may be written more than once, which is an error. */
  readonly primaryKeyCount: number;
  readonly isNotNull: boolean;
  readonly isUnique: boolean;
  readonly isAutoincrement: boolean;
  /** The last DEFAULT, as in SQLite, or null. */
  readonly defaultValue: ParsedExpression | null;
}

type DefaultConstraint = Extract<ColumnConstraint, { type: "DEFAULT" }>;

export function columnConstraints(column: ColumnDefinition): ColumnConstraints {
  const { constraints } = column;
  const hasConstraint = (type: ColumnConstraint["type"]): boolean =>
    constraints.some((constraint) => constraint.type === type);
  const lastDefault = constraints.findLast(
    (constraint): constraint is DefaultConstraint =>
      constraint.type === "DEFAULT",
  );
  return {
    primaryKeyCount: constraints.filter(
      (constraint) => constraint.type === "PRIMARY KEY",
    ).length,
    isNotNull: hasConstraint("NOT NULL"),
    isUnique: hasConstraint("UNIQUE"),
    isAutoincrement: hasConstraint("AUTOINCREMENT"),
    defaultValue: lastDefault?.value ?? null,
  };
}
