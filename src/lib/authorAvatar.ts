import type { ListAuthor } from "./types";

export type AuthorAvatarMode = "photo" | "initials" | "icon";

export function getAuthorInitials(name: string | null | undefined): string {
  const normalizedName = name?.trim();
  if (!normalizedName) {
    return "";
  }

  const words = normalizedName
    .split(/[\s._-]+/u)
    .map((word) => Array.from(word.replace(/[^\p{L}\p{N}]/gu, "")))
    .filter((letters) => letters.length > 0);

  if (words.length === 0) {
    return "";
  }

  const initials =
    words.length === 1 ? words[0].slice(0, 2) : [words[0][0], words[words.length - 1][0]];

  return initials.join("").toUpperCase();
}

export function getAuthorAvatarMode(author: ListAuthor | null | undefined): AuthorAvatarMode {
  if (author?.avatar_mode === "initials" || author?.avatar_mode === "icon") {
    return author.avatar_mode;
  }
  return author?.photo_path?.trim() ? "photo" : "icon";
}
