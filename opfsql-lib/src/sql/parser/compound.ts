import type {
  SelectNode,
  SetOperationNode,
  SetOperationType,
} from "./types.js";

// ---------------------------------------------------------------------------
// A compound query is a sequence of SELECTs, not nesting: its tree leans
// left one node per operator, and whatever walks it — the printer, the
// binder — reads it as a list, in a loop, so a long chain costs no stack.
// ---------------------------------------------------------------------------

/** A SELECT of a compound after the first, and the operator joining it on. */
export interface CompoundLink {
  readonly operator: SetOperationType;
  readonly select: SelectNode;
}

/** A body's SELECTs in order: the first, then each joined on by its operator. */
export interface CompoundMembers {
  readonly first: SelectNode;
  readonly links: readonly CompoundLink[];
}

export function compoundMembers(
  body: SelectNode | SetOperationNode,
): CompoundMembers {
  const links: CompoundLink[] = [];
  let member = body;
  while (member.type === "SET_OPERATION_NODE") {
    links.push({ operator: member.setOperationType, select: member.right });
    member = member.left;
  }
  return { first: member, links: links.toReversed() };
}
