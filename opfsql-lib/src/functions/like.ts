import { returns, strict, text } from "./arguments.js";
import type { CallSite, Implementation, ScalarFunction } from "./types.js";

// ---------------------------------------------------------------------------
// LIKE, as SQLite matches it: `%` is any run of characters, `_` one
// character, ASCII letters match either case, other characters only
// themselves. `text LIKE pattern` is bound as SQLite's like(pattern, text).
// ESCAPE is not supported (docs/semantics.md).
// ---------------------------------------------------------------------------

const REGEX_SPECIAL = /[$()*+.?[\\\]^{|}]/u;
const ASCII_LETTER = /^[a-z]$/iu;

/** A matcher for a LIKE pattern. */
function likeMatcher(pattern: string): (input: string) => boolean {
  let source = "";
  for (const character of pattern) source += wildcardOrLiteral(character);
  const regex = new RegExp(`^${source}$`, "su");
  return (input) => regex.test(input);
}

function wildcardOrLiteral(character: string): string {
  switch (character) {
    case "%": {
      return ".*";
    }
    case "_": {
      return ".";
    }
    default: {
      return literal(character);
    }
  }
}

function literal(character: string): string {
  if (REGEX_SPECIAL.test(character)) return `\\${character}`;
  if (!ASCII_LETTER.test(character)) return character;
  return `[${character.toLowerCase()}${character.toUpperCase()}]`;
}

/**
 * Compiles the pattern once when it is the same for every row; otherwise
 * remembers the last one, which helps when consecutive rows share it.
 */
function implementLike({ constants }: CallSite): Implementation {
  const [patternConstant] = constants;
  if (typeof patternConstant === "string") {
    const matches = likeMatcher(patternConstant);
    return strict(([, input]) => matches(text(input)));
  }
  const matcherFor = cachedMatcher();
  return strict(([pattern, input]) => matcherFor(text(pattern))(text(input)));
}

/** likeMatcher that keeps the matcher of the last pattern it was asked for. */
function cachedMatcher(): (pattern: string) => (input: string) => boolean {
  let last: { pattern: string; matches: (input: string) => boolean } | null =
    null;
  return (pattern) => {
    if (last?.pattern !== pattern) {
      last = { pattern, matches: likeMatcher(pattern) };
    }
    return last.matches;
  };
}

const like: ScalarFunction = {
  kind: "scalar",
  name: "like",
  signature: { required: ["text", "text"] },
  returnType: returns("BOOLEAN"),
  implement: implementLike,
};

export const likeFunctions: readonly ScalarFunction[] = [like];
