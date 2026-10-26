// Where a scenario's CPU time goes, phase by phase. Runs one scenario as
// `npm run bench` does — Engine.open on opfs-mock — profiles only its timed
// runs with the V8 sampling profiler, and gives each sample the phase of the
// nearest enclosing function that PHASE_ROOTS names: compiling the plan,
// running it, building the result, and the engine's work around them.
//
//   npm run bench:profile -- --scenario "prepared point query" --runs 20
//
// --detail lists the functions of the phases it names (compile,run by
// default), as shares of all samples.
// --out saves the raw .cpuprofile, which Chrome DevTools opens. Under
// vite-node every import goes through a getter (`get` in the listings),
// which a bundle would not have.
import "opfs-mock";
import "../test/setup/web-locks.js";
import { writeFileSync } from "node:fs";
import { Session as Inspector } from "node:inspector/promises";
import { parseArgs } from "node:util";
import { Engine } from "../src/worker-entry.js";
import { type Scenario, SCENARIOS } from "./scenarios.js";

interface CallFrame {
  readonly functionName: string;
  readonly url: string;
  readonly lineNumber: number;
}

interface ProfileNode {
  readonly id: number;
  readonly callFrame: CallFrame;
  readonly children?: readonly number[];
}

interface Profile {
  readonly nodes: readonly ProfileNode[];
  readonly samples: readonly number[];
}

/** A function that starts a phase: everything it calls counts toward it. */
interface PhaseRoot {
  readonly phase: string;
  readonly functionName: string;
  /** The end of its file's path, so that one name in two files is two roots. */
  readonly file: string;
}

const PHASE_ROOTS: readonly PhaseRoot[] = [
  root("compile", "statementContext", "executor/compile.ts"),
  root("compile", "compilePlan", "executor/compile.ts"),
  root("compile", "createRun", "expressions/scope.ts"),
  root("compile", "compileExpression", "expressions/compile.ts"),
  root("compile", "compilePredicate", "expressions/compile.ts"),
  root("run", "drain", "executor/operator.ts"),
  root("result", "rowsResult", "engine/results.ts"),
  root("result", "toData", "engine/api/result.ts"),
  root("plan (cached)", "plan", "engine/statements.ts"),
  root("parameters", "parametersOf", "values/parameters.ts"),
  root("pin a snapshot", "beginRead", "engine/database.ts"),
  root("schema cache", "load", "engine/schemas.ts"),
  root("transaction", "begin", "engine/transaction.ts"),
  root("end the transaction", "commit", "engine/transaction.ts"),
  root("end the transaction", "rollback", "engine/transaction.ts"),
];

/** Outside every phase root: the engine around them. */
const OUTSIDE = "engine: session/queue/async";

/** Samples V8 attributes to itself, whatever is on the stack. */
const OWN_PHASES = new Map([
  ["(garbage collector)", "garbage collection"],
  ["(program)", "(program)"],
  ["(idle)", "(idle)"],
]);

interface Settings {
  readonly scenario: Scenario;
  readonly runs: number;
  readonly warmup: number;
  /** Microseconds between samples. */
  readonly interval: number;
  /** Functions listed in each table of self time. */
  readonly top: number;
  /** Phases whose functions are listed after the totals. */
  readonly detail: readonly string[];
  readonly out: string | undefined;
}

await main(settingsOf(process.argv.slice(2)));

async function main(settings: Settings): Promise<void> {
  const profile = await profileRuns(settings);
  if (settings.out !== undefined) {
    writeFileSync(settings.out, JSON.stringify(profile));
  }
  const total = profile.samples.length;
  const phaseOf = phaseByNode(profile);
  print(`${settings.scenario.name}: ${String(total)} samples`);
  print(formatShares(countBy(profile.samples, phaseOf), total));
  print("\ntop self time");
  print(formatShares(selfTimes(profile, profile.samples), total, settings.top));
  for (const phase of settings.detail) {
    const samples = profile.samples.filter((id) => phaseOf(id) === phase);
    print(`\nwithin ${phase}, as shares of all samples`);
    print(formatShares(selfTimes(profile, samples), total, settings.top));
  }
}

function settingsOf(argv: readonly string[]): Settings {
  const { values } = parseArgs({
    args: argv.filter((argument) => argument !== "--"),
    options: {
      scenario: { type: "string", default: "" },
      runs: { type: "string", default: "20" },
      warmup: { type: "string", default: "3" },
      interval: { type: "string", default: "50" },
      top: { type: "string", default: "25" },
      detail: { type: "string", default: "compile,run" },
      out: { type: "string" },
    },
  });
  const wanted = values.scenario;
  const scenario = SCENARIOS.find(
    (candidate) => wanted !== "" && candidate.name.includes(wanted),
  );
  if (scenario === undefined) {
    throw new Error(`no scenario matches --scenario "${wanted}"`);
  }
  return {
    scenario,
    runs: Number(values.runs),
    warmup: Number(values.warmup),
    interval: Number(values.interval),
    top: Number(values.top),
    detail: values.detail.split(","),
    out: values.out,
  };
}

/** Profiles `runs` runs of the scenario, after `warmup` unprofiled ones. */
async function profileRuns(settings: Settings): Promise<Profile> {
  const { scenario } = settings;
  const engine = await Engine.open(`profile-${String(Date.now())}`);
  const setup = engine.connect();
  await scenario.setup(setup);
  await setup.close();
  const repeat = async (times: number): Promise<void> => {
    for (let count = 0; count < times; count++) {
      const session = engine.connect();
      await scenario.run(session);
      await session.close();
    }
  };
  await repeat(settings.warmup);
  const inspector = new Inspector();
  inspector.connect();
  await inspector.post("Profiler.enable");
  await inspector.post("Profiler.setSamplingInterval", {
    interval: settings.interval,
  });
  await inspector.post("Profiler.start");
  await repeat(settings.runs);
  const { profile } = await inspector.post("Profiler.stop");
  inspector.disconnect();
  await engine.close();
  return profile as Profile;
}

/** The phase of a sample's node: V8's own, or its nearest phase root's. */
function phaseByNode(profile: Profile): (id: number) => string {
  const parents = parentsOf(profile);
  const nodes = nodesById(profile);
  const known = new Map<number, string>();
  const phaseOf = (id: number): string => {
    const remembered = known.get(id);
    if (remembered !== undefined) return remembered;
    const frame = nodes.get(id)?.callFrame;
    const own = frame === undefined ? undefined : framePhase(frame);
    const parent = parents.get(id);
    const phase = own ?? (parent === undefined ? OUTSIDE : phaseOf(parent));
    known.set(id, phase);
    return phase;
  };
  return phaseOf;
}

function framePhase(frame: CallFrame): string | undefined {
  const own = OWN_PHASES.get(frame.functionName);
  if (own !== undefined) return own;
  return PHASE_ROOTS.find(
    (phaseRoot) =>
      phaseRoot.functionName === frame.functionName &&
      frame.url.endsWith(phaseRoot.file),
  )?.phase;
}

/** Samples by the function they were taken in: its name, file and line. */
function selfTimes(
  profile: Profile,
  samples: readonly number[],
): Map<string, number> {
  const nodes = nodesById(profile);
  return countBy(samples, (id) => {
    const frame = nodes.get(id)?.callFrame;
    if (frame === undefined) return "?";
    const file = frame.url.replace(/^.*\/(src|bench|node_modules)\//u, "$1/");
    const name = frame.functionName === "" ? "(anonymous)" : frame.functionName;
    return `${name}  ${file}:${String(frame.lineNumber + 1)}`;
  });
}

function nodesById(profile: Profile): Map<number, ProfileNode> {
  return new Map(profile.nodes.map((node) => [node.id, node]));
}

function parentsOf(profile: Profile): Map<number, number> {
  const parents = new Map<number, number>();
  for (const node of profile.nodes) {
    const children = node.children ?? [];
    for (const child of children) parents.set(child, node.id);
  }
  return parents;
}

function countBy<T>(
  items: readonly T[],
  key: (item: T) => string,
): Map<string, number> {
  const counts = new Map<string, number>();
  for (const item of items) {
    const name = key(item);
    counts.set(name, (counts.get(name) ?? 0) + 1);
  }
  return counts;
}

/** One line per name, largest first, as a share of `total` samples. */
function formatShares(
  counts: Map<string, number>,
  total: number,
  limit = Infinity,
): string {
  const largest = [...counts].toSorted(([, a], [, b]) => b - a);
  return largest
    .slice(0, limit)
    .map(([name, count]) => {
      const share = ((count / total) * 100).toFixed(1).padStart(5);
      return `${share}%  ${name}`;
    })
    .join("\n");
}

function print(text: string): void {
  process.stdout.write(`${text}\n`);
}

function root(phase: string, functionName: string, file: string): PhaseRoot {
  return { phase, functionName, file };
}
