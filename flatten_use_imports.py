#!/usr/bin/env python3
"""
Flatten nested `use` groups so each statement has at most one `{...}` at the end,
with no nested braces. Siblings from the same module stay grouped.

Run from the repo root:

    python3 scripts/flatten_use_imports.py

Touches *.rs files (skips target/). Review with git diff before committing.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def strip_use_comments(s: str) -> str:
    out = []
    i = 0
    n = len(s)
    while i < n:
        if s[i : i + 2] == "//":
            while i < n and s[i] != "\n":
                i += 1
            continue
        if s[i : i + 2] == "/*":
            j = s.find("*/", i + 2)
            i = j + 2 if j >= 0 else n
            continue
        out.append(s[i])
        i += 1
    return "".join(out)


def split_top_level_commas(s: str) -> list[str]:
    items = []
    depth = 0
    da = 0
    start = 0
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == "<":
            da += 1
        elif c == ">" and da > 0:
            da -= 1
        elif da == 0:
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            elif c == "," and depth == 0:
                items.append(s[start:i].strip())
                start = i + 1
        i += 1
    tail = s[start:].strip()
    if tail:
        items.append(tail)
    return items


def split_trailing_group(path: str) -> tuple[str, str | None]:
    """
    If path ends with ::{ ... } (balanced), return (prefix, inner) for the
    outermost such suffix. Otherwise (path, None).
    """
    path = path.strip()
    if not path.endswith("}"):
        return path, None
    close_end = len(path) - 1
    i = close_end
    depth = 0
    da = 0
    while i >= 0:
        c = path[i]
        if c == ">":
            da += 1
        elif c == "<" and da > 0:
            da -= 1
        elif da == 0:
            if c == "}":
                depth += 1
            elif c == "{":
                depth -= 1
                if depth == 0:
                    open_brace = i
                    if open_brace >= 2 and path[open_brace - 2 : open_brace] == "::":
                        prefix = path[: open_brace - 2].strip()
                        inner = path[open_brace + 1 : close_end].strip()
                        return prefix, inner
                    return path, None
        i -= 1
    return path, None


def parse_visibility_and_use(stmt: str) -> tuple[str, str] | None:
    """
    Return (vis_prefix, rest) where rest starts after 'use '.
    vis_prefix may be '' or 'pub ' or 'pub(crate) ' etc.
    """
    s = stmt.strip()
    m = re.match(r"^(pub(?:\([^)]*\))?\s+)?use\s+", s)
    if not m:
        return None
    vis = (m.group(1) or "").strip()
    if vis:
        vis = vis + " "
    rest = s[m.end() :].strip()
    if rest.endswith(";"):
        rest = rest[:-1].strip()
    return vis, rest


def has_nested_brace(rest: str) -> bool:
    """True if use tree has { ... { ... } ... }"""
    depth = 0
    da = 0
    for c in rest:
        if c == "<":
            da += 1
        elif c == ">" and da > 0:
            da -= 1
        elif da == 0:
            if c == "{":
                depth += 1
                if depth >= 2:
                    return True
            elif c == "}":
                depth -= 1
    return False


def path_items_need_split(rest: str) -> bool:
    """True if there's a top-level ::{ with comma-items containing :: or {."""
    prefix, inner = split_trailing_group(rest)
    if inner is None:
        return False
    for it in split_top_level_commas(inner):
        it = it.strip()
        if not it:
            continue
        if "::" in it or "{" in it:
            return True
    return False


def needs_flatten(rest: str) -> bool:
    return has_nested_brace(rest) or path_items_need_split(rest)


def normalize_self_item(prefix: str, item: str) -> str | None:
    """item is like 'self', 'self as Foo', or starts with self::"""
    item = item.strip()
    if item == "self":
        return prefix
    if item.startswith("self as "):
        alias = item[len("self as ") :].strip()
        return f"{prefix} as {alias}"
    if item.startswith("self::"):
        return f"{prefix}::{item[len('self::'):]}"
    return None


def expand_import_lines(prefix: str, inner: str) -> list[str]:
    """
    prefix: Rust path without trailing ::
    inner: comma-separated list inside outermost group for this prefix
    Returns list of path strings (each may end with ::{a,b} for grouping).
    """
    results: list[str] = []
    simple_names: list[str] = []

    def flush_simple():
        nonlocal simple_names
        if not simple_names:
            return
        if len(simple_names) == 1:
            results.append(f"{prefix}::{simple_names[0]}")
        else:
            results.append(f"{prefix}::{{{', '.join(simple_names)}}}")
        simple_names = []

    for raw in split_top_level_commas(inner):
        item = raw.strip()
        if not item:
            continue
        sub_pre, sub_in = split_trailing_group(item)
        if sub_in is not None:
            flush_simple()
            new_prefix = f"{prefix}::{sub_pre}" if sub_pre else prefix
            results.extend(expand_import_lines(new_prefix, sub_in))
            continue
        if "::" in item:
            flush_simple()
            results.append(f"{prefix}::{item}")
            continue
        ns = normalize_self_item(prefix, item)
        if ns is not None:
            flush_simple()
            results.append(ns)
            continue
        simple_names.append(item)

    flush_simple()
    return results


def merge_same_prefix(lines: list[str]) -> list[str]:
    """Merge use std::a; use std::b -> use std::{a, b} when possible."""
    from collections import defaultdict

    grouped: dict[str, list[str]] = defaultdict(list)
    order: list[str] = []
    singles: list[str] = []

    for line in lines:
        if "::{" in line:
            prefix, inner = split_trailing_group(line)
            if inner is None:
                singles.append(line)
                continue
            for name in split_top_level_commas(inner):
                name = name.strip()
                if not name:
                    continue
                if prefix not in order:
                    order.append(prefix)
                grouped[prefix].append(name)
        else:
            idx = line.rfind("::")
            if idx < 0:
                singles.append(line)
                continue
            p = line[:idx]
            last = line[idx + 2 :]
            if p not in order:
                order.append(p)
            grouped[p].append(last)

    out: list[str] = []
    out.extend(singles)
    for p in order:
        names = grouped[p]
        if not names:
            continue
        if len(names) == 1:
            out.append(f"{p}::{names[0]}")
        else:
            out.append(f"{p}::{{{', '.join(names)}}}")
    return out


def flatten_use_statement(vis: str, rest: str) -> list[str]:
    rest = rest.strip()
    if not needs_flatten(rest):
        return [f"{vis}use {rest};"]

    prefix, inner = split_trailing_group(rest)
    if inner is None:
        return [f"{vis}use {rest};"]

    parts = expand_import_lines(prefix, inner)
    parts = merge_same_prefix(parts)
    return [f"{vis}use {p};" for p in parts]


def find_use_statements(text: str) -> list[tuple[int, int, str]]:
    """Return list of (start, end, full_stmt) for top-level use statements."""
    results = []
    lines = text.split("\n")
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.strip()
        if stripped.startswith("//") or stripped.startswith("/*"):
            i += 1
            continue
        if not stripped.startswith("use ") and not re.match(
            r"^pub(?:\([^)]*\))?\s+use\s+", stripped
        ):
            i += 1
            continue
        start_line = i
        buf = [line]
        joined = line
        depth = joined.count("{") - joined.count("}")
        while not joined.rstrip().endswith(";") or depth != 0:
            i += 1
            if i >= n:
                break
            line = lines[i]
            buf.append(line)
            joined = "\n".join(buf)
            depth = joined.count("{") - joined.count("}")
        end_line = i
        stmt = "\n".join(buf)
        char_start = sum(len(lines[j]) + 1 for j in range(start_line))
        char_end = char_start + len(stmt)
        results.append((char_start, char_end, stmt))
        i = end_line + 1
    return results


def process_file(path: Path) -> bool:
    text = path.read_text()
    original = text
    uses = find_use_statements(text)
    if not uses:
        return False

    replacements: list[tuple[int, int, str]] = []
    for start, end, stmt in uses:
        cleaned = strip_use_comments(stmt)
        parsed = parse_visibility_and_use(cleaned)
        if not parsed:
            continue
        vis, rest = parsed
        if not needs_flatten(rest):
            continue
        try:
            new_stmts = flatten_use_statement(vis, rest)
        except ValueError as e:
            print(f"{path}: skip: {e}", file=sys.stderr)
            continue
        new_block = "\n".join(new_stmts)
        replacements.append((start, end, new_block))

    if not replacements:
        return False

    for start, end, new_block in reversed(replacements):
        text = text[:start] + new_block + text[end:]

    if text != original:
        path.write_text(text)
        return True
    return False


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    changed = []
    for path in sorted(root.rglob("*.rs")):
        if "target" in path.parts:
            continue
        if process_file(path):
            changed.append(path)
    for p in changed:
        print(p.relative_to(root))


if __name__ == "__main__":
    main()
