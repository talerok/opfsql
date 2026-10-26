import { logical, type physical } from "../plan/index.js";
import { chooseAccessPath } from "./access-path.js";
import { NOTHING, unknownEstimate } from "./cost.js";
import { emptyPlan } from "./empty.js";
import {
  hasNeverTrue,
  isPinned,
  readsOnly,
  tablesOf,
} from "./expression-tree.js";
import { joinPair, type Planned, type Side } from "./join-pair.js";
import { lowerExpressions } from "./lower-expression.js";
import type { WantedOrder } from "./order-match.js";
import { type TableRead, tableReadOf } from "./plan-tree.js";
import type { Planner } from "./planner.js";

// ---------------------------------------------------------------------------
// Pass 3, join planning (docs/planning.md, Physical plans). An inner join's
// sides may be swapped, and a chain of inner joins reordered: expressions
// above find columns by binding, not by position. The chain's relations
// and the conditions of all its joins are gathered; the relation read most
// selectively on its own leads (fewest rows by cost.ts: a rowid or a
// unique key, then an equality with a constant or a parameter, then other
// seeks, then a full read), and the others join it one at a time, left-deep,
// each next the one whose join costs least among those a condition connects
// to what is joined so far. Each join is planned by join-pair.ts: the new
// relation looked up when it can be, otherwise both read once. A condition
// joins at the first join that has every table it reads.
//
// A LEFT join keeps its sides: its left side is planned on its own, its
// right side looked up or read once. A join whose conditions hold a
// volatile call or a correlated subquery is not reordered with the joins
// around it, so such a condition still runs at the join it was written on;
// its own two sides may still swap.
//
// An ORDER BY over a join may ask one table to lead, read in its order
// (order.ts): it leads then, and the joins after it keep its order when
// each is a lookup join (rule 3).
// ---------------------------------------------------------------------------

type Expression = physical.Expression;

/** The table asked to lead a join, and the order it is asked for. */
export interface Lead {
  readonly tableIndex: number;
  readonly order: WantedOrder;
}

/** How join planning lowers what is neither a join nor a table read. */
export interface JoinPlanning {
  readonly planner: Planner;
  readonly lower: (plan: logical.Plan) => physical.Plan;
}

/** One relation of a join chain, as written. */
interface Relation {
  readonly plan: logical.Plan;
  /** The tables whose columns it gives. */
  readonly tables: ReadonlySet<number>;
  /** A table read with its filter, which a seek can find rows of. */
  readonly read: TableRead | null;
}

/** The relations joined so far, as one plan. */
interface Chain {
  readonly planned: Planned;
  readonly tables: ReadonlySet<number>;
}

export function planJoin(
  join: logical.Join,
  planning: JoinPlanning,
  lead: Lead | null,
): Planned {
  if (hasNeverTrue(join.conditions)) return emptyJoin(join, planning);
  if (join.joinType === "left") return planLeftJoin(join, planning, lead);
  const relations: Relation[] = [];
  const conditions: logical.Expression[] = [];
  gather(join, relations, conditions);
  const lowered = lowerExpressions(conditions, planning.planner);
  return planChain(relations, lowered, planning, lead);
}

/**
 * A join whose ON no pair passes: nothing for an inner join, and for a left
 * join every left row with NULLs, the right side read as empty.
 */
function emptyJoin(join: logical.Join, planning: JoinPlanning): Planned {
  if (join.joinType === "inner") {
    return { plan: emptyPlan(join), estimate: NOTHING, isOrdered: false };
  }
  const left = planAlone(relationOf(join.left), planning, null);
  const conditions = lowerExpressions(join.conditions, planning.planner);
  const right = emptyPlan(join.right);
  return {
    plan: {
      kind: "nested-loop-join",
      joinType: "left",
      left: left.plan,
      right,
      conditions,
    },
    estimate: left.estimate,
    isOrdered: false,
  };
}

function planLeftJoin(
  join: logical.Join,
  planning: JoinPlanning,
  lead: Lead | null,
): Planned {
  const left = planAlone(relationOf(join.left), planning, lead);
  const conditions = lowerExpressions(join.conditions, planning.planner);
  const right = sideOf(relationOf(join.right), planning);
  const pair = { joinType: "left", left, right, conditions } as const;
  return joinPair(pair, planning.planner);
}

/** The relations of an inner join and their conditions, through the joins below it that may be reordered with it. */
function gather(
  join: logical.Join,
  relations: Relation[],
  conditions: logical.Expression[],
): void {
  conditions.push(...join.conditions);
  for (const side of [join.left, join.right]) {
    if (isReorderable(join) && isReorderable(side)) {
      gather(side, relations, conditions);
    } else {
      relations.push(relationOf(side));
    }
  }
}

/** An inner join whose conditions may run at another join of its chain. */
function isReorderable(plan: logical.Plan): plan is logical.Join {
  if (plan.kind !== "join" || plan.joinType !== "inner") return false;
  const { conditions } = plan;
  const isMovable = conditions.every((condition) => !isPinned(condition));
  return !hasNeverTrue(conditions) && isMovable;
}

function relationOf(plan: logical.Plan): Relation {
  const read = tableReadOf(plan);
  const isRead = read !== null && !hasNeverTrue(read.conditions);
  const tables = tablesOf(logical.layoutOf(plan));
  return { plan, tables, read: isRead ? read : null };
}

/** The chain planned: its lead first, then each relation joined to it. */
function planChain(
  relations: readonly Relation[],
  conditions: readonly Expression[],
  planning: JoinPlanning,
  lead: Lead | null,
): Planned {
  const alone = new Map<Relation, Planned>();
  const aloneOf = (relation: Relation): Planned => {
    const known = alone.get(relation) ?? planAlone(relation, planning, null);
    alone.set(relation, known);
    return known;
  };
  const leading = leadOf(relations, lead);
  const first = leading ?? mostSelective(relations, aloneOf);
  const start =
    leading === undefined ? aloneOf(first) : planAlone(first, planning, lead);
  let chain: Chain = { planned: start, tables: first.tables };
  let remaining = relations.filter((relation) => relation !== first);
  let pending = conditions;
  while (remaining.length > 0) {
    const step = nextStep(chain, remaining, pending, { planning, aloneOf });
    chain = step.chain;
    remaining = remaining.filter((relation) => relation !== step.relation);
    pending = pending.filter((condition) => !step.used.includes(condition));
  }
  return withLeftovers(chain.planned, pending);
}

/** The relation holding the table asked to lead, if one does. */
function leadOf(
  relations: readonly Relation[],
  lead: Lead | null,
): Relation | undefined {
  if (lead === null) return undefined;
  return relations.find((relation) => relation.tables.has(lead.tableIndex));
}

/** The relation whose own read gives the fewest rows, the first on a tie. */
function mostSelective(
  relations: readonly Relation[],
  aloneOf: (relation: Relation) => Planned,
): Relation {
  const [first, ...others] = relations;
  if (first === undefined) throw new RangeError("a join of no relations");
  let best = first;
  for (const relation of others) {
    const rows = aloneOf(relation).estimate.rows;
    if (rows < aloneOf(best).estimate.rows) best = relation;
  }
  return best;
}

/** One join of the chain: the relation it adds, the conditions it checks. */
interface Step {
  readonly relation: Relation;
  readonly used: readonly Expression[];
  readonly chain: Chain;
}

interface StepPlanning {
  readonly planning: JoinPlanning;
  readonly aloneOf: (relation: Relation) => Planned;
}

/** The cheapest next join among the relations a condition connects to the chain; any, if none does. */
function nextStep(
  chain: Chain,
  remaining: readonly Relation[],
  pending: readonly Expression[],
  { planning, aloneOf }: StepPlanning,
): Step {
  const connected = remaining.filter((relation) =>
    pending.some((condition) => connects(condition, chain, relation)),
  );
  const candidates = connected.length > 0 ? connected : remaining;
  let best: Step | null = null;
  for (const relation of candidates) {
    const tables = new Set([...chain.tables, ...relation.tables]);
    const used = pending.filter((condition) => readsOnly(condition, tables));
    const right = { read: relation.read, alone: () => aloneOf(relation) };
    const pair = {
      joinType: "inner",
      left: chain.planned,
      right,
      conditions: used,
    } as const;
    const planned = joinPair(pair, planning.planner);
    const isCheaper =
      best === null || planned.estimate.cost < best.chain.planned.estimate.cost;
    if (isCheaper) best = { relation, used, chain: { planned, tables } };
  }
  if (best === null) throw new RangeError("no relation left to join");
  return best;
}

/** True if `condition` reads the relation and only tables joined with it. */
function connects(
  condition: Expression,
  chain: Chain,
  relation: Relation,
): boolean {
  const tables = new Set([...chain.tables, ...relation.tables]);
  return readsOnly(condition, tables) && !readsOnly(condition, chain.tables);
}

/** Conditions no join of the chain could check, which every condition reads: kept above it. */
function withLeftovers(
  planned: Planned,
  conditions: readonly Expression[],
): Planned {
  if (conditions.length === 0) return planned;
  const plan: physical.Plan = {
    kind: "filter",
    child: planned.plan,
    conditions,
  };
  return { ...planned, plan };
}

/** A relation as a join's right side. */
function sideOf(relation: Relation, planning: JoinPlanning): Side {
  return {
    read: relation.read,
    alone: () => planAlone(relation, planning, null),
  };
}

/** A relation read on its own, in the order asked if it leads. */
function planAlone(
  relation: Relation,
  planning: JoinPlanning,
  lead: Lead | null,
): Planned {
  const { plan, read } = relation;
  if (read !== null) return planRead(read, planning.planner, lead);
  if (plan.kind === "join") return planJoin(plan, planning, lead);
  const estimate = hasNeverTrueFilter(plan) ? NOTHING : unknownEstimate();
  return { plan: planning.lower(plan), estimate, isOrdered: false };
}

function planRead(
  read: TableRead,
  planner: Planner,
  lead: Lead | null,
): Planned {
  const { get } = read;
  const isLead = lead !== null && lead.tableIndex === get.tableIndex;
  const conditions = lowerExpressions(read.conditions, planner);
  const order = isLead ? lead.order : null;
  const path = chooseAccessPath({ get, conditions, order }, planner);
  return {
    plan: path.plan,
    estimate: path.estimate,
    isOrdered: path.isOrdered,
  };
}

function hasNeverTrueFilter(plan: logical.Plan): boolean {
  return plan.kind === "filter" && hasNeverTrue(plan.conditions);
}
