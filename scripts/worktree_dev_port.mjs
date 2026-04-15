import { createHash } from "node:crypto";
import { resolve } from "node:path";

export const DEFAULT_WORKTREE_DEV_BASE_PORT = 4321;
export const DEFAULT_WORKTREE_DEV_PORT_SPAN = 1000;
export const WORKTREE_DEV_BASE_PORT_ENV = "WORKTREE_DEV_BASE_PORT";
export const WORKTREE_DEV_PORT_ENV = "WORKTREE_DEV_PORT";
export const WORKTREE_DEV_PORT_OFFSET_ENV = "WORKTREE_DEV_PORT_OFFSET";
export const WORKTREE_DEV_PORT_SPAN_ENV = "WORKTREE_DEV_PORT_SPAN";
export const WORKTREE_DEV_ROOT_ENV = "WORKTREE_DEV_ROOT";

const MAX_PORT = 65535;

function parsePortLike(value, name) {
  if (value === undefined || value === "") {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_PORT) {
    throw new Error(`${name} must be an integer from 1 to ${MAX_PORT}.`);
  }

  return parsed;
}

function parsePositiveInteger(value, name) {
  if (value === undefined || value === "") {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer.`);
  }

  return parsed;
}

export function worktreePathKey(worktreeRoot) {
  return resolve(worktreeRoot).split(/[\\/]+/).filter(Boolean).join("/");
}

export function worktreePortOffset(worktreeRoot, span) {
  const digest = createHash("sha256").update(worktreePathKey(worktreeRoot)).digest();
  return digest.readUInt32BE(0) % span;
}

export function resolveWorktreeDevPort({ env = process.env, worktreeRoot }) {
  if (!worktreeRoot) {
    throw new Error("worktreeRoot is required to resolve the dev server port.");
  }

  const explicitPort =
    parsePortLike(env[WORKTREE_DEV_PORT_ENV], WORKTREE_DEV_PORT_ENV) ??
    parsePortLike(env.PORT, "PORT");

  const pathKey = worktreePathKey(worktreeRoot);
  if (explicitPort !== undefined) {
    const span = DEFAULT_WORKTREE_DEV_PORT_SPAN;
    return {
      basePort: DEFAULT_WORKTREE_DEV_BASE_PORT,
      offset: worktreePortOffset(worktreeRoot, span),
      pathKey,
      port: explicitPort,
      span,
      usingExplicitPort: true,
    };
  }

  const basePort =
    parsePortLike(env[WORKTREE_DEV_BASE_PORT_ENV], WORKTREE_DEV_BASE_PORT_ENV) ??
    DEFAULT_WORKTREE_DEV_BASE_PORT;
  const span =
    parsePositiveInteger(env[WORKTREE_DEV_PORT_SPAN_ENV], WORKTREE_DEV_PORT_SPAN_ENV) ??
    DEFAULT_WORKTREE_DEV_PORT_SPAN;

  if (basePort + span - 1 > MAX_PORT) {
    throw new Error(
      `${WORKTREE_DEV_BASE_PORT_ENV} + ${WORKTREE_DEV_PORT_SPAN_ENV} - 1 must not exceed ${MAX_PORT}.`,
    );
  }

  const offset = worktreePortOffset(worktreeRoot, span);
  const derivedPort = basePort + offset;

  return {
    basePort,
    offset,
    pathKey,
    port: explicitPort ?? derivedPort,
    span,
    usingExplicitPort: false,
  };
}
