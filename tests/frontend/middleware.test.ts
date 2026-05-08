import { describe, expect, it, vi } from "vitest";

vi.mock("astro:middleware", () => ({
  defineMiddleware: (handler: unknown) => handler,
}));

import { onRequest } from "../../src/middleware";

function createContext(pathname: string) {
  const redirect = vi.fn((location: string, status: number) => ({ location, status }));

  return {
    url: new URL(`https://example.com${pathname}`),
    redirect,
  };
}

describe("middleware redirects", () => {
  it("redirects the encoded Conductor ANSI reset alias to home", async () => {
    const context = createContext("/%1B%5B39m");
    const next = vi.fn();

    const response = await onRequest(context as never, next);

    expect(context.redirect).toHaveBeenCalledWith("/", 302);
    expect(next).not.toHaveBeenCalled();
    expect(response).toEqual({ location: "/", status: 302 });
  });

  it("passes through normal paths", async () => {
    const context = createContext("/guides/spokane");
    const nextResponse = new Response("ok");
    const next = vi.fn(async () => nextResponse);

    const response = await onRequest(context as never, next);

    expect(context.redirect).not.toHaveBeenCalled();
    expect(next).toHaveBeenCalledOnce();
    expect(response).toBe(nextResponse);
  });
});
