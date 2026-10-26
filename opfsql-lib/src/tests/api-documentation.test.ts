import { readFileSync } from "node:fs";
import path from "node:path";
import {
  createProgram,
  ModuleKind,
  ModuleResolutionKind,
  ScriptTarget,
} from "typescript";
import { describe, expect, it } from "vitest";

// docs/api.md is the contract of the package's public API. Its "Exports"
// section lists every name each entry point exports, and the rest of it
// names no type or class that neither exports.

const ROOT = path.join(import.meta.dirname, "..", "..");
const API_DOC = readFileSync(path.join(ROOT, "docs", "api.md"), "utf8");

const ENTRY_POINTS = [
  { name: "opfsql", file: "src/index.ts" },
  { name: "opfsql/worker", file: "src/worker-entry.ts" },
] as const;

/** The web platform's names that api.md mentions; opfsql exports none. */
const PLATFORM_NAMES = new Set([
  "AbortSignal",
  "ArrayBuffer",
  "Date",
  "Infinity",
  "Map",
  "Promise",
  "Symbol",
  "Uint8Array",
]);

/** A PascalCase word: how a class or type is named, unlike SQL keywords. */
const TYPE_NAME = /\b[A-Z][a-z]\w*/gu;

/** Every name `file` exports, values and types alike, as the compiler sees them. */
function exportsOf(file: string): string[] {
  const fileName = path.join(ROOT, file);
  const program = createProgram([fileName], {
    module: ModuleKind.ESNext,
    moduleResolution: ModuleResolutionKind.Bundler,
    target: ScriptTarget.ES2022,
    noEmit: true,
    noLib: true,
    types: [],
  });
  const checker = program.getTypeChecker();
  const source = program.getSourceFile(fileName);
  const module =
    source === undefined ? undefined : checker.getSymbolAtLocation(source);
  if (module === undefined) throw new Error(`${file} is not a module`);
  return checker.getExportsOfModule(module).map((symbol) => symbol.name);
}

/** The text of the section that starts with `heading`, up to the next one at its level. */
function section(text: string, heading: string): string {
  const start = text.indexOf(`\n${heading}\n`);
  if (start === -1) throw new Error(`api.md has no "${heading}"`);
  const level = heading.slice(0, heading.indexOf(" ") + 1);
  const end = text.indexOf(`\n${level}`, start + heading.length + 1);
  if (end === -1) return text.slice(start);
  return text.slice(start, end);
}

/** The names in the first cell of each row of the Exports table of `entry`. */
function documentedExports(entry: string): string[] {
  const exports = section(API_DOC, "## Exports");
  const table = section(exports, `### \`"${entry}"\``);
  return table
    .split("\n")
    .filter((line) => line.startsWith("| `"))
    .flatMap((line) => codeSpans(line.split("|", 2)[1] ?? ""));
}

function codeSpans(text: string): string[] {
  const spans = text.match(/`[^`]+`/gu) ?? [];
  return spans.map((span) => span.slice(1, -1));
}

/** api.md without its Exports section: the prose and examples. */
function prose(): string {
  const exports = section(API_DOC, "## Exports");
  return API_DOC.replace(exports, "");
}

/** Type and class names in code spans and declared in code blocks. */
function namesMentioned(text: string): Set<string> {
  const blocks = text.match(/```ts\n[\s\S]*?```/gu) ?? [];
  const declarations = blocks.flatMap(
    (block) => block.match(/\b(?:class|interface|type) \w+/gu) ?? [],
  );
  const declared = declarations.map((declaration) =>
    declaration.slice(declaration.indexOf(" ") + 1),
  );
  const inline = codeSpans(text.replaceAll(/```[\s\S]*?```/gu, "")).flatMap(
    (span) => span.match(TYPE_NAME) ?? [],
  );
  return new Set([...declared, ...inline]);
}

function sorted(names: readonly string[]): string[] {
  return names.toSorted((a, b) => a.localeCompare(b));
}

describe("docs/api.md", () => {
  for (const { name, file } of ENTRY_POINTS) {
    it(`lists exactly what "${name}" exports`, () => {
      const documented = documentedExports(name);
      expect(sorted(documented)).toEqual(sorted(exportsOf(file)));
    });
  }

  it("names no type or class that the package does not export", () => {
    const exported = new Set(
      ENTRY_POINTS.flatMap(({ file }) => exportsOf(file)),
    );
    const unknown = [...namesMentioned(prose())].filter(
      (name) => !exported.has(name) && !PLATFORM_NAMES.has(name),
    );
    expect(unknown).toEqual([]);
  });
});
