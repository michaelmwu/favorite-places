import { defineMiddleware } from "astro:middleware";

// Conductor's Open button can append this ANSI reset fragment to the dev URL.
const redirectedPaths = new Set(["/%1B%5B39m", "/%1b%5b39m", "/\u001b[39m"]);

const shouldRedirectToHome = (pathname: string) => {
  if (redirectedPaths.has(pathname)) {
    return true;
  }

  try {
    return redirectedPaths.has(decodeURIComponent(pathname));
  } catch {
    return false;
  }
};

export const onRequest = defineMiddleware((context, next) => {
  if (shouldRedirectToHome(context.url.pathname)) {
    return context.redirect("/", 302);
  }

  return next();
});
