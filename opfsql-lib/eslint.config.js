// @ts-check
import js from "@eslint/js";
import prettier from "eslint-config-prettier/flat";
import { createTypeScriptImportResolver } from "eslint-import-resolver-typescript";
import { importX } from "eslint-plugin-import-x";
import { configs as sonarjs } from "eslint-plugin-sonarjs";
import unicorn from "eslint-plugin-unicorn";
import { config, configs as tseslint } from "typescript-eslint";

// ---------------------------------------------------------------------------
// As strict as the plugins go. Violations in code written before this config
// are recorded in eslint-suppressions.json: new code must be clean, and the
// file only shrinks (`npm run lint:prune` drops what has been fixed).
// Formatting is Prettier's: the rules it decides are turned off here.
// ---------------------------------------------------------------------------

export default config(
  {
    ignores: ["dist/", "coverage/", "test-results/", "playwright-report/"],
  },
  js.configs.recommended,
  tseslint.strictTypeChecked,
  tseslint.stylisticTypeChecked,
  sonarjs.recommended,
  unicorn.configs.recommended,
  importX.flatConfigs.recommended,
  importX.flatConfigs.typescript,
  {
    languageOptions: {
      parserOptions: {
        // Code under the strict compiler flags gets its types from there.
        project: ["./tsconfig.strict.json", "./tsconfig.check.json"],
        tsconfigRootDir: import.meta.dirname,
      },
    },
    settings: {
      "import-x/resolver-next": [
        createTypeScriptImportResolver({ project: "./tsconfig.check.json" }),
      ],
    },
    linterOptions: {
      reportUnusedDisableDirectives: "error",
    },
    rules: {
      // --- Size and complexity ----------------------------------------------
      // Cognitive, not cyclomatic, complexity: an exhaustive switch over a
      // union is the intended way to dispatch, however many cases it has.
      "sonarjs/cognitive-complexity": ["error", 11],
      "max-depth": ["error", 3],
      "max-params": ["error", 4],
      "max-nested-callbacks": ["error", 3],
      "max-lines-per-function": [
        "error",
        { max: 50, skipBlankLines: true, skipComments: true },
      ],
      "max-lines": [
        "error",
        { max: 400, skipBlankLines: true, skipComments: true },
      ],

      // --- Correctness and explicitness -------------------------------------
      eqeqeq: ["error", "always"],
      curly: ["error", "multi-line", "consistent"],
      "no-console": ["error", { allow: ["warn", "error"] }],
      // A parameter is never reassigned, nor its object changed — except
      // byte buffers, which functions are given precisely to fill.
      "no-param-reassign": [
        "error",
        {
          props: true,
          ignorePropertyModificationsFor: [
            "bytes",
            "page",
            "map",
            "target",
            "buffer",
          ],
        },
      ],
      "no-implicit-coercion": "error",
      "no-else-return": ["error", { allowElseIf: false }],
      "no-nested-ternary": "error",
      "object-shorthand": "error",
      "prefer-template": "error",
      "@typescript-eslint/explicit-function-return-type": [
        "error",
        { allowExpressions: true, allowTypedFunctionExpressions: true },
      ],
      "@typescript-eslint/explicit-member-accessibility": [
        "error",
        { accessibility: "no-public" },
      ],
      // One way to hide a member: TypeScript's `private`, not `#name`.
      "no-restricted-syntax": [
        "error",
        {
          selector: "PrivateIdentifier",
          message: "Use TypeScript's `private` modifier, not a `#` field.",
        },
      ],
      "@typescript-eslint/consistent-type-imports": "error",
      "@typescript-eslint/consistent-type-exports": "error",
      "@typescript-eslint/switch-exhaustiveness-check": [
        "error",
        { considerDefaultExhaustiveForUnions: false },
      ],
      "@typescript-eslint/strict-boolean-expressions": [
        "error",
        { allowString: false, allowNumber: false, allowNullableObject: false },
      ],
      "@typescript-eslint/prefer-readonly": "error",
      "@typescript-eslint/no-shadow": "error",
      "@typescript-eslint/no-unnecessary-qualifier": "error",
      "@typescript-eslint/promise-function-async": "error",
      "@typescript-eslint/require-array-sort-compare": "error",
      "@typescript-eslint/naming-convention": [
        "error",
        { selector: "default", format: ["camelCase"] },
        { selector: "import", format: null },
        {
          selector: "variable",
          modifiers: ["const", "global"],
          format: ["camelCase", "UPPER_CASE"],
        },
        { selector: "typeLike", format: ["PascalCase"] },
        {
          selector: "interface",
          format: ["PascalCase"],
          custom: { regex: "^I[A-Z]", match: false },
        },
        { selector: "enumMember", format: ["PascalCase", "UPPER_CASE"] },
        {
          selector: ["objectLiteralProperty", "typeProperty"],
          modifiers: ["requiresQuotes"],
          format: null,
        },
        {
          selector: "parameter",
          modifiers: ["unused"],
          format: ["camelCase"],
          leadingUnderscore: "require",
        },
      ],

      // --- Modules ----------------------------------------------------------
      "import-x/no-cycle": "error",
      "import-x/no-duplicates": "error",
      "import-x/no-named-as-default": "error",
      "import-x/no-named-as-default-member": "error",
      "import-x/no-default-export": "error",
      "import-x/no-extraneous-dependencies": "error",
      "import-x/no-mutable-exports": "error",
      "import-x/no-self-import": "error",
      // docs/architecture.md, Modules: dependencies of rebuilt modules.
      "import-x/no-restricted-paths": [
        "error",
        {
          zones: [
            {
              target: "./src/values",
              from: "./src",
              except: ["./values", "./errors.ts"],
              message: "values depends on nothing but errors.",
            },
            {
              target: "./src/storage",
              from: "./src",
              except: ["./storage", "./values", "./errors.ts"],
              message: "storage depends on nothing but values and errors.",
            },
            {
              target: "./src/schema",
              from: "./src",
              except: ["./schema", "./values", "./errors.ts"],
              message: "schema depends on nothing but values and errors.",
            },
            {
              target: "./src/table",
              from: "./src",
              except: [
                "./table",
                "./storage",
                "./schema",
                "./functions",
                "./values",
                "./errors.ts",
              ],
              message:
                "table depends on nothing but storage, schema, functions, values and errors.",
            },
            {
              target: "./src/errors.ts",
              from: "./src",
              except: ["./errors.ts"],
              message: "errors depends on nothing.",
            },
            {
              target: "./src/functions",
              from: "./src",
              except: ["./functions", "./values", "./errors.ts"],
              message: "functions depends on nothing but values and errors.",
            },
            {
              target: "./src/sql/plan",
              from: "./src",
              except: [
                "./sql/plan",
                "./schema",
                "./functions",
                "./values",
                "./errors.ts",
              ],
              message:
                "sql/plan depends on nothing but schema, functions, values and errors.",
            },
            {
              target: "./src/expressions",
              from: "./src",
              except: [
                "./expressions",
                "./sql/plan",
                "./functions",
                "./values",
                "./errors.ts",
              ],
              message:
                "expressions depends on nothing but sql/plan, functions, values and errors.",
            },
            {
              target: "./src/executor",
              from: "./src",
              except: [
                "./executor",
                "./sql/plan",
                "./expressions",
                "./table",
                "./functions",
                "./schema",
                "./values",
                "./errors.ts",
              ],
              message:
                "executor depends on nothing but sql/plan, expressions, table, functions, schema, values and errors.",
            },
            {
              target: "./src/sql/binder",
              from: "./src",
              except: [
                "./sql/binder",
                "./sql/parser",
                "./sql/plan",
                "./schema",
                "./functions",
                "./values",
                "./errors.ts",
              ],
              message:
                "sql/binder depends on nothing but sql/parser, sql/plan, schema, functions, values and errors.",
            },
            {
              target: "./src/sql/parser",
              from: "./src",
              except: ["./sql/parser", "./values", "./errors.ts"],
              message: "sql/parser depends on nothing but values and errors.",
            },
            {
              target: "./src/engine/api",
              from: "./src",
              except: ["./engine/api", "./values", "./errors.ts"],
              message:
                "engine/api, the engine's contract, depends only on values and errors.",
            },
            {
              // The worker reaches the engine only through its contract, so
              // the page's bundle need not carry the engine (tests aside).
              target: "./src/worker/*.ts",
              from: "./src",
              except: ["./worker", "./engine/api", "./values", "./errors.ts"],
              message: "worker depends only on engine/api, values and errors.",
            },
          ],
        },
      ],

      // Spreading a Uint8Array gives a number[]: .slice() is the copy.
      "unicorn/prefer-spread": "off",
      // Plans and expressions are trees: recursing into a child reads as
      // the structure does, where a loop would hide it.
      "unicorn/no-useless-recursion": "off",
      // Guard returns read better than ternaries over several lines.
      "unicorn/prefer-ternary": "off",
      // SQL NULL is null; undefined means "absent", never a value.
      "unicorn/no-null": "off",
      "unicorn/filename-case": ["error", { case: "kebabCase" }],
      // There is no DOM here: plan nodes have `children` too.
      "unicorn/better-dom-traversing": "off",
      // Iterator helpers (zip, toArray) are not in the browsers opfsql
      // supports yet, nor in its TypeScript library.
      "unicorn/prefer-iterator-zip": "off",
      "unicorn/prefer-iterator-to-array": "off",
      // A function named as a predicate must return a boolean, but a function
      // that returns one need not be a predicate (a conversion, say).
      "unicorn/consistent-boolean-name": [
        "error",
        { checkFunctions: "prohibit", checkMethods: "prohibit" },
      ],
      // SQL values are a union, and so is what most of the engine returns.
      "sonarjs/function-return-type": "off",
      // A class reads from its public surface down to its details.
      "unicorn/consistent-class-member-order": [
        "error",
        {
          order: [
            "static-field",
            "static-block",
            "public-field",
            "private-field",
            "constructor",
            "static-method",
            "public-method",
            "private-method",
          ],
        },
      ],
      // One-line /** */ and " * "-prefixed lines are the TSDoc conventions
      // that editors and type tooling read.
      "unicorn/single-line-block-comment-style": "off",
      "unicorn/no-asterisk-prefix-in-documentation-comments": "off",
    },
  },
  {
    // Tests read as scenarios: long bodies and repeated literals are fine.
    files: ["**/*.test.ts", "test/**/*.ts", "bench/**/*.ts"],
    rules: {
      "max-lines": "off",
      "max-lines-per-function": "off",
      "max-nested-callbacks": "off",
      "sonarjs/no-duplicate-string": "off",
      "unicorn/max-nested-calls": "off",
      // let x; beforeEach(() => { x = ... }) is how vitest sets up state.
      "unicorn/no-top-level-assignment-in-function": "off",
      "@typescript-eslint/no-non-null-assertion": "off",
      "@typescript-eslint/explicit-function-return-type": "off",
    },
  },
  prettier,
  {
    files: ["eslint.config.js"],
    extends: [tseslint.disableTypeChecked],
    // A flat config is a module whose default export is built at load time.
    rules: {
      "import-x/no-default-export": "off",
      "unicorn/no-top-level-side-effects": "off",
    },
  },
);
