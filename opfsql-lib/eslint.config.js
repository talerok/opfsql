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
      "no-param-reassign": ["error", { props: true }],
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
          ],
        },
      ],

      // SQL NULL is null; undefined means "absent", never a value.
      "unicorn/no-null": "off",
      "unicorn/filename-case": ["error", { case: "kebabCase" }],
      // There is no DOM here: plan nodes have `children` too.
      "unicorn/better-dom-traversing": "off",
      // Iterator.zip is not in the browsers opfsql supports yet.
      "unicorn/prefer-iterator-zip": "off",
      // A function named as a predicate must return a boolean, but a function
      // that returns one need not be a predicate (a conversion, say).
      "unicorn/consistent-boolean-name": [
        "error",
        { checkFunctions: "prohibit", checkMethods: "prohibit" },
      ],
      // SQL values are a union, and so is what most of the engine returns.
      "sonarjs/function-return-type": "off",
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
