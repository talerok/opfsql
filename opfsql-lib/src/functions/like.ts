import { SemanticError } from "../errors.js";
import type { Value } from "../values/index.js";
import { characters, returns, strict, text } from "./arguments.js";
import type { CallSite, Implementation, ScalarFunction } from "./types.js";

// ---------------------------------------------------------------------------
// LIKE, as SQLite matches it: `%` is any run of characters, `_` one
// character, ASCII letters match either case, other characters only
// themselves. The function form is SQLite's: like(pattern, text [, escape])
// is `text LIKE pattern ESCAPE escape`.
// ---------------------------------------------------------------------------

const REGEX_SPECIAL = /[$()*+.?[\\\]^{|}]/u;
const ASCII_LETTER = /^[a-z]$/iu;

/**
 * A matcher for a LIKE pattern. `escape`, if given, is one character that
 * makes the character after it literal.
 */
export function likeMatcher(
  pattern: string,
  escape?: string,
): (input: string) => boolean {
  if (escape !== undefined && characters(escape).length !== 1) {
    throw new SemanticError("ESCAPE expression must be a single character");
  }
  const regex = likeRegex(pattern, escape);
  return (input) => regex.test(input);
}

function likeRegex(pattern: string, escape: string | undefined): RegExp {
  let source = "";
  let isEscaped = false;
  for (const character of pattern) {
    if (isEscaped) {
      source += literal(character);
      isEscaped = false;
    } else if (character === escape) {
      isEscaped = true;
    } else {
      source += wildcardOrLiteral(character);
    }
  }
  // A pattern ending in the escape character matches nothing, as in SQLite.
  return isEscaped ? /(?!)/u : new RegExp(`^${source}$`, "su");
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
  return ASCII_LETTER.test(character)
    ? `[${character.toLowerCase()}${character.toUpperCase()}]`
    : character;
}

/**
 * Compiles the pattern once when it is the same for every row; otherwise
 * keeps the last one, which helps when consecutive rows share a pattern.
 */
function implementLike({ constants }: CallSite): Implementation {
  const [patternConstant, , escapeConstant] = constants;
  const hasFixedEscape = constants.length < 3 || escapeConstant !== undefined;
  if (
    typeof patternConstant === "string" &&
    hasFixedEscape &&
    escapeConstant !== null
  ) {
    const matches = likeMatcher(patternConstant, escapeText(escapeConstant));
    return strict(([, input]) => matches(text(input)));
  }
  let last: { key: string; matches: (input: string) => boolean } | null = null;
  return strict(([pattern, input, escape]) => {
    const patternText = text(pattern);
    const escapeCharacter = escapeText(escape);
    const key = `${escapeCharacter ?? ""}\0${patternText}`;
    if (last?.key !== key) {
      last = { key, matches: likeMatcher(patternText, escapeCharacter) };
    }
    return last.matches(text(input));
  });
}

function escapeText(escape: Value | undefined): string | undefined {
  return escape === undefined || escape === null ? undefined : text(escape);
}

const like: ScalarFunction = {
  kind: "scalar",
  name: "like",
  signature: { required: ["text", "text"], optional: ["text"] },
  returnType: returns("BOOLEAN"),
  implement: implementLike,
};

export const likeFunctions: readonly ScalarFunction[] = [like];
