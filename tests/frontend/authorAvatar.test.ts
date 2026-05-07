import { describe, expect, it } from "vitest";

import { getAuthorAvatarMode, getAuthorInitials } from "../../src/lib/authorAvatar";

describe("author avatar helpers", () => {
  it("builds one or two initials from author names", () => {
    expect(getAuthorInitials("Another Author")).toBe("AA");
    expect(getAuthorInitials("Example Curator")).toBe("EC");
    expect(getAuthorInitials("Prince")).toBe("PR");
  });

  it("uses explicit avatar mode before photo fallback", () => {
    expect(getAuthorAvatarMode({ name: "Another Author", avatar_mode: "initials" })).toBe(
      "initials",
    );
    expect(getAuthorAvatarMode({ name: "Example Curator", photo_path: "/author.svg" })).toBe(
      "photo",
    );
    expect(getAuthorAvatarMode({ name: "Example Curator" })).toBe("icon");
  });
});
