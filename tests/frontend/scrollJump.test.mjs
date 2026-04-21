import { describe, expect, it } from "vitest";

import {
  getScrollBehavior,
  getScrollControlAnchor,
  isScrollControlDisabled,
  SCROLL_SHOW_OFFSET,
  shouldShowScrollControl,
} from "../../public/scripts/scroll-jump-core.js";

describe("scroll jump", () => {
  it("shows the control only after both the page length and current scroll clear the threshold", () => {
    expect(
      shouldShowScrollControl({
        maxScroll: SCROLL_SHOW_OFFSET,
        scrollTop: SCROLL_SHOW_OFFSET + 1,
      }),
    ).toBe(false);
    expect(
      shouldShowScrollControl({
        maxScroll: SCROLL_SHOW_OFFSET + 1,
        scrollTop: SCROLL_SHOW_OFFSET,
      }),
    ).toBe(false);
    expect(
      shouldShowScrollControl({
        maxScroll: SCROLL_SHOW_OFFSET + 1,
        scrollTop: SCROLL_SHOW_OFFSET + 1,
      }),
    ).toBe(true);
  });

  it("uses reduced-motion aware scroll behavior", () => {
    expect(getScrollBehavior(true)).toBe("auto");
    expect(getScrollBehavior(false)).toBe("smooth");
  });

  it("disables the top button when already near the top", () => {
    expect(isScrollControlDisabled(0)).toBe(true);
    expect(isScrollControlDisabled(79)).toBe(true);
    expect(isScrollControlDisabled(80)).toBe(false);
  });

  it("anchors away from the map when an expanded panel is present", () => {
    expect(getScrollControlAnchor([])).toBe("right");
    expect(getScrollControlAnchor([{ dataset: { collapsed: "true" } }])).toBe("right");
    expect(getScrollControlAnchor([{ dataset: { collapsed: "false" } }])).toBe("left");
    expect(
      getScrollControlAnchor([
        { dataset: { collapsed: "true" } },
        { dataset: { collapsed: "false" } },
      ]),
    ).toBe("left");
  });
});
