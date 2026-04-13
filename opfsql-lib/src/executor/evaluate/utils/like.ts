/** Convert SQL LIKE pattern to RegExp. */
export function likeToRegex(pattern: string): RegExp {
  let re = "^";
  for (let i = 0; i < pattern.length; i++) {
    const ch = pattern[i];
    if (ch === "%") {
      re += ".*";
    } else if (ch === "_") {
      re += ".";
    } else if (/[.*+?^${}()|[\]\\]/.test(ch)) {
      re += "\\" + ch;
    } else {
      re += ch;
    }
  }
  re += "$";
  return new RegExp(re);
}
