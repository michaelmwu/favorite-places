import { spawn, spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";

import {
  resolveWorktreeDevPort,
  WORKTREE_DEV_PORT_ENV,
  WORKTREE_DEV_PORT_OFFSET_ENV,
  WORKTREE_DEV_ROOT_ENV,
} from "./worktree_dev_port.mjs";

const PORT_ARG_NAMES = new Set(["--port", "-p"]);

function gitWorktreeRoot() {
  const result = spawnSync("git", ["rev-parse", "--show-toplevel"], {
    encoding: "utf8",
  });

  if (result.status === 0) {
    return result.stdout.trim();
  }

  return process.cwd();
}

function parseCliPort(args) {
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg.startsWith("--port=")) {
      return arg.slice("--port=".length);
    }

    if (PORT_ARG_NAMES.has(arg)) {
      return args[index + 1];
    }
  }

  return undefined;
}

const forwardedArgs = process.argv.slice(2);
const cliPort = parseCliPort(forwardedArgs);
const worktreeRoot = gitWorktreeRoot();
const portConfig = resolveWorktreeDevPort({
  env: cliPort === undefined ? process.env : { ...process.env, [WORKTREE_DEV_PORT_ENV]: cliPort },
  worktreeRoot,
});

const astroArgs = ["dev"];
if (cliPort === undefined) {
  astroArgs.push("--port", String(portConfig.port));
}
astroArgs.push(...forwardedArgs);

const childEnv = {
  ...process.env,
  PORT: String(portConfig.port),
  [WORKTREE_DEV_PORT_ENV]: String(portConfig.port),
  [WORKTREE_DEV_PORT_OFFSET_ENV]: String(portConfig.offset),
  [WORKTREE_DEV_ROOT_ENV]: worktreeRoot,
};

const astroBin = join(
  worktreeRoot,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "astro.cmd" : "astro",
);
const command = existsSync(astroBin) ? astroBin : "astro";
const source = portConfig.usingExplicitPort
  ? "explicit port"
  : `base ${portConfig.basePort} + offset ${portConfig.offset}`;

console.log(`Starting Astro on port ${portConfig.port} (${source})`);

const child = spawn(command, astroArgs, {
  env: childEnv,
  stdio: "inherit",
});

child.on("error", (error) => {
  console.error(`Failed to start Astro: ${error.message}`);
  process.exit(1);
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }

  process.exit(code ?? 0);
});
