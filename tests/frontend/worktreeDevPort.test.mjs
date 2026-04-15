import { describe, expect, it } from "vitest";

import {
  DEFAULT_WORKTREE_DEV_BASE_PORT,
  DEFAULT_WORKTREE_DEV_PORT_SPAN,
  resolveWorktreeDevPort,
  worktreePathKey,
} from "../../scripts/worktree_dev_port.mjs";

describe("worktree dev port", () => {
  it("derives a stable port from the worktree root path", () => {
    const first = resolveWorktreeDevPort({ env: {}, worktreeRoot: "/repo/alpha" });
    const second = resolveWorktreeDevPort({ env: {}, worktreeRoot: "/repo/alpha" });

    expect(second).toEqual(first);
    expect(first.port).toBeGreaterThanOrEqual(DEFAULT_WORKTREE_DEV_BASE_PORT);
    expect(first.port).toBeLessThan(
      DEFAULT_WORKTREE_DEV_BASE_PORT + DEFAULT_WORKTREE_DEV_PORT_SPAN,
    );
  });

  it("uses configured base port and span for the deterministic offset", () => {
    const config = resolveWorktreeDevPort({
      env: {
        WORKTREE_DEV_BASE_PORT: "5100",
        WORKTREE_DEV_PORT_SPAN: "50",
      },
      worktreeRoot: "/repo/beta",
    });

    expect(config.basePort).toBe(5100);
    expect(config.span).toBe(50);
    expect(config.port).toBe(5100 + config.offset);
    expect(config.offset).toBeGreaterThanOrEqual(0);
    expect(config.offset).toBeLessThan(50);
  });

  it("lets an explicit worktree port override the derived port", () => {
    const config = resolveWorktreeDevPort({
      env: { WORKTREE_DEV_PORT: "6200" },
      worktreeRoot: "/repo/gamma",
    });

    expect(config.port).toBe(6200);
    expect(config.usingExplicitPort).toBe(true);
  });

  it("uses PORT as a fallback explicit override", () => {
    const config = resolveWorktreeDevPort({
      env: { PORT: "6300" },
      worktreeRoot: "/repo/delta",
    });

    expect(config.port).toBe(6300);
    expect(config.usingExplicitPort).toBe(true);
  });

  it("normalizes paths into a filesystem-root-relative key", () => {
    expect(worktreePathKey("/Users/example/project")).toBe("Users/example/project");
  });
});
