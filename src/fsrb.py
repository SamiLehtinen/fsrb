#!/usr/bin/env python3
r"""
fsrb - File Search Replace Block (Python)

A strict, deterministic, delimiter-based “Search & Replace” patch applier with
global all-or-nothing transactionality.

Patch format:

    path/to/target.ext
    <<<<<<< SEARCH
    [Exact original content to find]
    =======
    [New content to replace with]
    >>>>>>> REPLACE

Escaping delimiter lines inside SEARCH/REPLACE content:

If you need a literal line that would otherwise be interpreted as a delimiter
line, prefix it with a single backslash at the start of the line:

    \<<<<<<< SEARCH
    \=======
    \>>>>>>> REPLACE

During parsing, the leading "\" is removed and the literal delimiter text is
included in the block content. The unescaped delimiter lines still terminate
blocks as normal.

Notes:
- This version intentionally DOES NOT support the legacy header format
  "# file: path". Use the plain path header line only.
- Matching is literal (no regex), overlap-aware for uniqueness, and performed on
  LF-normalized content in memory; original file line endings are preserved.
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple


# --- Data Structures ---


@dataclass(frozen=True)
class PatchBlock:
    file_path: str
    # Normalized to LF (`\n`) regardless of patch file line endings.
    search_pattern: str
    # Normalized to LF (`\n`) regardless of patch file line endings.
    replace_pattern: str
    # 1-based index of the block within the patch file
    block_index: int
    # Line number in patch file where this block starts (for debugging)
    line_number: int


class ValidationErrorType(Enum):
    FileNotFound = "FileNotFound"
    FileUnreadable = "FileUnreadable"
    EmptySearchBlock = "EmptySearchBlock"
    SearchBlockNotFound = "SearchBlockNotFound"
    AmbiguousMatch = "AmbiguousMatch"
    InvalidEmptySearchAndReplace = "InvalidEmptySearchAndReplace"
    CreateOnExistingFile = "CreateOnExistingFile"
    CreateNotFirstBlock = "CreateNotFirstBlock"


@dataclass(frozen=True)
class ValidationError:
    file_path: str
    block_index: int
    error_type: ValidationErrorType
    message: str
    snippet: str


class LineEnding(Enum):
    Lf = "Lf"
    Crlf = "Crlf"


@dataclass
class FileState:
    path: str
    # Original line ending style detected from disk content.
    line_ending: LineEnding
    # Normalized file content using LF (`\n`) for matching/applying.
    content_norm: str
    # Whether the file existed on disk at validation time.
    existed_on_disk: bool = True
    # Whether it has been modified from the original disk state.
    modified: bool = False


# --- Helpers: Line Endings ---


def detect_line_ending(content: str) -> LineEnding:
    return LineEnding.Crlf if "\r\n" in content else LineEnding.Lf


def normalize_to_lf(s: str) -> str:
    # Convert CRLF -> LF. Leave lone CR untouched (avoid destructive transforms).
    return s.replace("\r\n", "\n")


def lf_to_crlf(s: str) -> str:
    # Convert LF -> CRLF. Assumes input does not contain CRLF already.
    return s.replace("\n", "\r\n")


# --- Helpers: Matching / Replacement ---


class UniqueMatchKind(Enum):
    None_ = "None"
    One = "One"
    Ambiguous = "Ambiguous"


@dataclass(frozen=True)
class UniqueMatch:
    kind: UniqueMatchKind
    start: Optional[int] = None
    count: Optional[int] = None


def find_unique_overlapping(haystack: str, needle: str) -> UniqueMatch:
    """
    Overlap-aware substring uniqueness check.
    Returns unique start index if exactly one occurrence exists.
    """
    if not needle or len(needle) > len(haystack):
        return UniqueMatch(UniqueMatchKind.None_)

    count = 0
    first_pos = None

    # Overlap-aware scan using str.find with incremental start.
    i = 0
    while True:
        pos = haystack.find(needle, i)
        if pos == -1:
            break
        count += 1
        if count == 1:
            first_pos = pos
        # allow overlaps by advancing just one char
        i = pos + 1

    if count == 0:
        return UniqueMatch(UniqueMatchKind.None_)
    if count == 1:
        return UniqueMatch(UniqueMatchKind.One, start=first_pos)
    return UniqueMatch(UniqueMatchKind.Ambiguous, count=count)


def replace_at(haystack: str, start: int, needle_len: int, replacement: str) -> str:
    return haystack[:start] + replacement + haystack[start + needle_len :]


def get_snippet(text: str) -> str:
    first_line = text.split("\n", 1)[0] if text else ""
    return (first_line[:60] + "...") if len(first_line) > 60 else first_line


# --- Parsing Logic ---


DELIM_SEARCH = "<<<<<<< SEARCH"
DELIM_SEP = "======="
DELIM_REPLACE = ">>>>>>> REPLACE"
DELIMITERS = {DELIM_SEARCH, DELIM_SEP, DELIM_REPLACE}


def _unescape_delimiter_line(line: str) -> str:
    """
    If a line is an escaped delimiter line (starts with "\" and then exactly one of the
    delimiter markers), return the unescaped marker. Otherwise return the line unchanged.

    Escape is only recognized when "\" is the very first character of the line to
    avoid destroying intended indentation.
    """
    if line.startswith("\\"):
        candidate = line[1:]
        if candidate in DELIMITERS:
            return candidate
    return line


def parse_patch_file(content: str) -> List[PatchBlock]:
    """
    Parses an fsrb patch file.

    Recognizes file headers only in the plain-path form:
        <path>   (as a line by itself)

    The header is only recognized if the next non-ignored line is exactly "<<<<<<< SEARCH".

    Lines that may be present and are ignored for header detection:
      - blank lines
      - comment lines beginning with "#"
      - Markdown code fences beginning with "```"

    Delimiter escaping inside SEARCH/REPLACE content is supported via leading "\" on
    a delimiter line (see module docstring).
    """
    blocks: List[PatchBlock] = []
    lines: List[str] = content.splitlines()

    current_file: Optional[str] = None
    block_counter = 0
    i = 0

    def is_ignored_line_for_header(t: str) -> bool:
        tt = t.strip()
        return tt == "" or tt.startswith("#") or tt.startswith("```")

    while i < len(lines):
        line = lines[i]
        trimmed = line.strip()

        # Skip markdown code fences (ignored as per spec) at top-level
        if trimmed.startswith("```"):
            i += 1
            continue

        # File header: plain path line
        if (
            trimmed
            and trimmed not in (DELIM_SEARCH, DELIM_SEP, DELIM_REPLACE)
            and not trimmed.startswith("#")
        ):
            j = i + 1
            while j < len(lines) and is_ignored_line_for_header(lines[j]):
                j += 1
            if j < len(lines) and lines[j].strip() == DELIM_SEARCH:
                current_file = trimmed
                i += 1
                continue

        # Search block
        if trimmed == DELIM_SEARCH:
            if not current_file:
                raise ValueError(
                    f"Line {i+1}: Found SEARCH block without a preceding file path header line."
                )

            block_counter += 1
            start_line = i + 1
            i += 1

            search_lines: List[str] = []
            while i < len(lines) and lines[i].strip() != DELIM_SEP:
                search_lines.append(_unescape_delimiter_line(lines[i]))
                i += 1

            if i >= len(lines):
                raise ValueError(
                    f"Block #{block_counter}: Unexpected end of file while reading SEARCH block."
                )

            # Skip "======="
            i += 1

            replace_lines: List[str] = []
            while i < len(lines) and lines[i].strip() != DELIM_REPLACE:
                replace_lines.append(_unescape_delimiter_line(lines[i]))
                i += 1

            if i >= len(lines):
                raise ValueError(
                    f"Block #{block_counter}: Unexpected end of file while reading REPLACE block."
                )

            # Skip ">>>>>>> REPLACE"
            i += 1

            # Normalize to LF explicitly via join("\n")
            search = normalize_to_lf("\n".join(search_lines))
            replace = normalize_to_lf("\n".join(replace_lines))

            blocks.append(
                PatchBlock(
                    file_path=current_file,
                    search_pattern=search,
                    replace_pattern=replace,
                    block_index=block_counter,
                    line_number=start_line,
                )
            )
            continue

        # Ignore other lines
        i += 1

    return blocks


# --- Validation & In-Memory Application ---


def validate_and_apply_in_memory(
    blocks: List[PatchBlock],
) -> Tuple[Dict[str, FileState], List[ValidationError]]:
    file_states: Dict[str, FileState] = {}
    errors: List[ValidationError] = []

    # Unique file paths in deterministic order.
    unique_paths = sorted({b.file_path for b in blocks})

    # file -> earliest block index referencing it (for consistent load errors)
    first_block_for_file: Dict[str, int] = {}
    for b in blocks:
        if b.file_path not in first_block_for_file:
            first_block_for_file[b.file_path] = b.block_index
        else:
            first_block_for_file[b.file_path] = min(first_block_for_file[b.file_path], b.block_index)

    # Pre-scan which files are intended to be created.
    create_files = {
        b.file_path
        for b in blocks
        if (b.search_pattern == "" and b.replace_pattern != "")
    }

    unavailable_files = set()

    # Load all referenced files (or create empty in-memory states for CREATE targets).
    for path in unique_paths:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8", errors="strict", newline="") as f:
                    content = f.read()
                le = detect_line_ending(content)
                content_norm = normalize_to_lf(content)
                file_states[path] = FileState(
                    path=path,
                    line_ending=le,
                    content_norm=content_norm,
                    existed_on_disk=True,
                    modified=False,
                )
            except Exception as e:
                errors.append(
                    ValidationError(
                        file_path=path,
                        block_index=first_block_for_file.get(path, 1),
                        error_type=ValidationErrorType.FileUnreadable,
                        message=f"Error reading file: {e}",
                        snippet="",
                    )
                )
                unavailable_files.add(path)
        else:
            if path in create_files:
                # CREATE targets start from an empty, non-existent state.
                file_states[path] = FileState(
                    path=path,
                    line_ending=LineEnding.Lf,
                    content_norm="",
                    existed_on_disk=False,
                    modified=False,
                )
            else:
                errors.append(
                    ValidationError(
                        file_path=path,
                        block_index=first_block_for_file.get(path, 1),
                        error_type=ValidationErrorType.FileNotFound,
                        message="File not found on disk.",
                        snippet="",
                    )
                )
                unavailable_files.add(path)

    # Track whether a CREATE has already been applied per file, and enforce ordering.
    seen_noncreate_for_file: Dict[str, bool] = {}
    seen_create_for_file: Dict[str, bool] = {}

    for block in blocks:
        if block.file_path in unavailable_files:
            continue

        state = file_states.get(block.file_path)
        if state is None:
            errors.append(
                ValidationError(
                    file_path=block.file_path,
                    block_index=block.block_index,
                    error_type=ValidationErrorType.FileUnreadable,
                    message="Internal error: file state not loaded.",
                    snippet="",
                )
            )
            continue

        is_create = (block.search_pattern == "" and block.replace_pattern != "")
        is_invalid_empty = (block.search_pattern == "" and block.replace_pattern == "")

        if is_invalid_empty:
            errors.append(
                ValidationError(
                    file_path=block.file_path,
                    block_index=block.block_index,
                    error_type=ValidationErrorType.InvalidEmptySearchAndReplace,
                    message="Invalid block: SEARCH and REPLACE are both empty.",
                    snippet="",
                )
            )
            continue

        if is_create:
            if state.existed_on_disk:
                errors.append(
                    ValidationError(
                        file_path=block.file_path,
                        block_index=block.block_index,
                        error_type=ValidationErrorType.CreateOnExistingFile,
                        message="CREATE block is only valid when the target file does not exist.",
                        snippet="",
                    )
                )
                continue

            if seen_noncreate_for_file.get(block.file_path, False):
                errors.append(
                    ValidationError(
                        file_path=block.file_path,
                        block_index=block.block_index,
                        error_type=ValidationErrorType.CreateNotFirstBlock,
                        message="CREATE block must be the first block for its file.",
                        snippet="",
                    )
                )
                continue

            if seen_create_for_file.get(block.file_path, False):
                errors.append(
                    ValidationError(
                        file_path=block.file_path,
                        block_index=block.block_index,
                        error_type=ValidationErrorType.AmbiguousMatch,
                        message="Multiple CREATE blocks for the same file are not allowed.",
                        snippet="",
                    )
                )
                continue

            # Apply CREATE by setting the entire file content from empty.
            state.content_norm = block.replace_pattern
            state.modified = True
            seen_create_for_file[block.file_path] = True
            continue

        # Non-CREATE blocks: enforce legacy rule that SEARCH must not be empty.
        if block.search_pattern == "":
            errors.append(
                ValidationError(
                    file_path=block.file_path,
                    block_index=block.block_index,
                    error_type=ValidationErrorType.EmptySearchBlock,
                    message="SEARCH block is empty.",
                    snippet="",
                )
            )
            continue

        seen_noncreate_for_file[block.file_path] = True

        m = find_unique_overlapping(state.content_norm, block.search_pattern)
        if m.kind == UniqueMatchKind.None_:
            errors.append(
                ValidationError(
                    file_path=block.file_path,
                    block_index=block.block_index,
                    error_type=ValidationErrorType.SearchBlockNotFound,
                    message="SEARCH block not found in file content.",
                    snippet=get_snippet(block.search_pattern),
                )
            )
        elif m.kind == UniqueMatchKind.Ambiguous:
            errors.append(
                ValidationError(
                    file_path=block.file_path,
                    block_index=block.block_index,
                    error_type=ValidationErrorType.AmbiguousMatch,
                    message=f"Ambiguous match. Found {m.count} occurrences.",
                    snippet=get_snippet(block.search_pattern),
                )
            )
        else:
            assert m.start is not None
            state.content_norm = replace_at(
                state.content_norm, m.start, len(block.search_pattern), block.replace_pattern
            )
            state.modified = True

    return file_states, errors


# --- Transactional Commit (All-or-Nothing, Atomic) ---


@dataclass
class CommitUnit:
    target_path: str
    final_content: str
    action: str  # 'update' | 'create' | 'delete'
    temp_path: Optional[str] = None
    backup_path: Optional[str] = None


def unique_sibling_path(target_path: str, tag: str) -> str:
    parent = os.path.dirname(target_path) or ""
    base = os.path.basename(target_path)
    pid = os.getpid()
    now_ns = time.time_ns()

    for n in range(0, 10_000):
        candidate = os.path.join(parent, f"{base}.{tag}.{pid}.{now_ns}.{n}")
        if not os.path.exists(candidate):
            return candidate

    raise FileExistsError(f"Unable to allocate unique temp path for {target_path}")


def sync_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if not parent:
        return
    try:
        fd = os.open(parent, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            os.fsync(fd)
        except OSError:
            pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def cleanup_leftovers(units: List[CommitUnit]) -> None:
    for u in units:
        if u.temp_path:
            try:
                os.remove(u.temp_path)
            except OSError:
                pass
        if u.backup_path:
            try:
                os.remove(u.backup_path)
            except OSError:
                pass


def rollback_swaps(
    units: List[CommitUnit], installed_news: List[str], moved_originals: List[str]
) -> Optional[str]:
    errs: List[str] = []

    # 1) For targets with new installed: restore backup over it (reverse order).
    for target in reversed(installed_news):
        unit = next((u for u in units if u.target_path == target), None)
        if not unit or not unit.backup_path:
            continue
        backup = unit.backup_path

        try:
            roll_tmp = unique_sibling_path(target, ".fsrb.rollback")
        except Exception as e:
            errs.append(f"Rollback: failed to create rollback temp path for {target}: {e}")
            continue

        try:
            os.rename(target, roll_tmp)
        except Exception as e:
            errs.append(f"Rollback: failed to move new target aside ({target}): {e}")
            continue

        try:
            os.rename(backup, target)
        except Exception as e:
            errs.append(f"Rollback: failed to restore backup ({backup} -> {target}): {e}")
            # Try to put new content back to avoid missing target.
            try:
                os.rename(roll_tmp, target)
            except Exception:
                pass
            continue

        try:
            os.remove(roll_tmp)
        except OSError:
            pass

        sync_parent_dir(target)

    # 2) For originals moved but possibly without new installed:
    for target in reversed(moved_originals):
        unit = next((u for u in units if u.target_path == target), None)
        if not unit or not unit.backup_path:
            continue
        backup = unit.backup_path

        if os.path.exists(target):
            continue
        if os.path.exists(backup):
            try:
                os.rename(backup, target)
                sync_parent_dir(target)
            except Exception as e:
                errs.append(f"Rollback: failed to restore moved original ({backup} -> {target}): {e}")

    return None if not errs else " | ".join(errs)


@dataclass
class CommitUnit:
    target_path: str
    final_content: str
    action: str  # 'update' | 'create' | 'delete'
    temp_path: Optional[str] = None
    backup_path: Optional[str] = None


def unique_sibling_path(target_path: str, tag: str) -> str:
    parent = os.path.dirname(target_path) or ""
    base = os.path.basename(target_path)
    pid = os.getpid()
    now_ns = time.time_ns()

    for n in range(0, 10_000):
        candidate = os.path.join(parent, f"{base}.{tag}.{pid}.{now_ns}.{n}")
        if not os.path.exists(candidate):
            return candidate

    raise FileExistsError(f"Unable to allocate unique temp path for {target_path}")


def sync_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if not parent:
        return

    try:
        fd = os.open(parent, os.O_RDONLY)
    except OSError:
        return

    try:
        try:
            os.fsync(fd)
        except OSError:
            pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _ensure_parent_dirs_for_create(target_path: str) -> List[str]:
    parent = os.path.dirname(target_path)
    if not parent:
        return []

    missing: List[str] = []
    p = parent
    while p and not os.path.exists(p):
        missing.append(p)
        new_p = os.path.dirname(p)
        if new_p == p:
            break
        p = new_p

    created: List[str] = []
    for d in reversed(missing):
        try:
            os.mkdir(d)
            created.append(d)
        except FileExistsError:
            pass

    return created


def _remove_empty_dirs_best_effort(dirs: List[str]) -> None:
    # Remove in reverse (deepest first).
    for d in reversed(dirs):
        try:
            os.rmdir(d)
        except OSError:
            pass


def _remove_empty_parent_dirs_best_effort(file_path: str) -> None:
    parent = os.path.dirname(file_path)
    while parent and parent not in (".", os.path.sep):
        try:
            os.rmdir(parent)
        except OSError:
            break
        parent = os.path.dirname(parent)


def cleanup_leftovers(units: List[CommitUnit]) -> None:
    for u in units:
        if u.temp_path:
            try:
                os.remove(u.temp_path)
            except OSError:
                pass

        if u.backup_path:
            try:
                os.remove(u.backup_path)
            except OSError:
                pass


def rollback_swaps(
    units: List[CommitUnit],
    installed_news: List[str],
    moved_originals: List[str],
) -> Optional[str]:
    errs: List[str] = []

    # 1) For targets with new installed: restore backup over it (reverse order).
    for target in reversed(installed_news):
        unit = next((u for u in units if u.target_path == target), None)
        if not unit:
            continue

        if unit.action == "create":
            # Roll back to non-existence.
            try:
                if os.path.exists(target):
                    os.remove(target)
                    sync_parent_dir(target)
            except Exception as e:
                errs.append(f"Rollback: failed to remove created file {target}: {e}")
            continue

        # update: restore backup over it.
        if not unit.backup_path:
            continue
        backup = unit.backup_path

        try:
            roll_tmp = unique_sibling_path(target, ".fsrb.rollback")
        except Exception as e:
            errs.append(f"Rollback: failed to create rollback temp path for {target}: {e}")
            continue

        try:
            os.rename(target, roll_tmp)
        except Exception as e:
            errs.append(f"Rollback: failed to move new target aside ({target}): {e}")
            continue

        try:
            os.rename(backup, target)
        except Exception as e:
            errs.append(f"Rollback: failed to restore backup ({backup} -> {target}): {e}")
            # Try to put new content back to avoid missing target.
            try:
                os.rename(roll_tmp, target)
            except Exception:
                pass
            continue

        try:
            os.remove(roll_tmp)
        except OSError:
            pass

        sync_parent_dir(target)

    # 2) For originals moved but possibly without new installed (delete):
    for target in reversed(moved_originals):
        unit = next((u for u in units if u.target_path == target), None)
        if not unit or not unit.backup_path:
            continue

        backup = unit.backup_path

        if os.path.exists(target):
            continue

        if os.path.exists(backup):
            try:
                os.rename(backup, target)
                sync_parent_dir(target)
            except Exception as e:
                errs.append(
                    f"Rollback: failed to restore moved original ({backup} -> {target}): {e}"
                )

    return None if not errs else " | ".join(errs)


def commit_changes_atomic(file_states: Dict[str, FileState], total_blocks: int) -> None:
    # Deterministic order.
    paths = sorted(file_states.keys())

    units: List[CommitUnit] = []
    for p in paths:
        st = file_states[p]
        if not st.modified:
            continue

        # Logic:
        # Create: if not existed_on_disk and content != ""
        # Delete: if existed_on_disk and content is empty/whitespace
        # Update: if existed_on_disk and content is not empty
        
        is_empty_content = (st.content_norm.strip() == "")

        if (not st.existed_on_disk) and not is_empty_content:
            action = "create"
        elif st.existed_on_disk and is_empty_content:
            action = "delete"
        elif st.existed_on_disk and not is_empty_content:
            action = "update"
        else:
            # e.g., created then deleted within the same transaction => no-op on disk
            continue

        final_content = st.content_norm if st.line_ending == LineEnding.Lf else lf_to_crlf(st.content_norm)
        units.append(CommitUnit(target_path=st.path, final_content=final_content, action=action))

    if not units:
        print()
        print(f"[SUCCESS] All {total_blocks} blocks applied to {0} files.")
        return

    # Phase A: create any needed parent dirs for CREATE targets, then write temp files.
    created_dirs: List[str] = []
    try:
        for unit in units:
            if unit.action == "create":
                created_dirs.extend(_ensure_parent_dirs_for_create(unit.target_path))

            if unit.action in ("create", "update"):
                target = unit.target_path
                parent = os.path.dirname(target)
                if parent and not os.path.exists(parent):
                    raise FileNotFoundError(f"Target directory does not exist: {parent}")

                temp = unique_sibling_path(target, ".fsrb.tmp")
                unit.temp_path = temp

                # Create new file exclusively.
                with open(temp, "xb") as f:
                    f.write(unit.final_content.encode("utf-8"))
                    f.flush()
                    os.fsync(f.fileno())

                # Preserve permissions best-effort (update only).
                if unit.action == "update":
                    try:
                        mode = os.stat(target).st_mode
                        os.chmod(temp, mode & 0o7777)
                    except OSError:
                        pass
    except Exception as e:
        cleanup_leftovers(units)
        _remove_empty_dirs_best_effort(created_dirs)
        raise

    # Phase B: swap in atomically per file with rollback on any failure.
    moved_originals: List[str] = []
    installed_news: List[str] = []

    try:
        for unit in units:
            target = unit.target_path

            if unit.action == "create":
                assert unit.temp_path is not None
                # Ensure target does not exist (strict CREATE semantics).
                if os.path.exists(target):
                    raise FileExistsError(f"CREATE target already exists: {target}")
                os.rename(unit.temp_path, target)
                installed_news.append(target)
                sync_parent_dir(target)
                continue

            if unit.action == "delete":
                # Move original out of the way; do not install a new file.
                if not os.path.exists(target):
                    raise FileNotFoundError(f"DELETE target missing at commit time: {target}")
                backup = unique_sibling_path(target, ".fsrb.bak")
                unit.backup_path = backup
                os.rename(target, backup)
                moved_originals.append(target)
                sync_parent_dir(target)
                continue

            # update
            assert unit.temp_path is not None
            backup = unique_sibling_path(target, ".fsrb.bak")
            unit.backup_path = backup

            os.rename(target, backup)
            moved_originals.append(target)

            os.rename(unit.temp_path, target)
            installed_news.append(target)
            sync_parent_dir(target)

    except Exception as e:
        rollback_err = rollback_swaps(units, installed_news, moved_originals)
        cleanup_leftovers(units)
        _remove_empty_dirs_best_effort(created_dirs)
        if rollback_err:
            raise RuntimeError(
                f"Commit failed: {e}. Rollback also encountered errors: {rollback_err}"
            ) from e
        raise

    # Phase C: delete backups and any leftover temps.
    cleanup_leftovers(units)

    # Post-commit: remove empty parent dirs for DELETE (best-effort).
    for unit in units:
        if unit.action == "delete" and unit.backup_path:
            _remove_empty_parent_dirs_best_effort(unit.target_path)

    for unit in units:
        print(f"Saved changes to {unit.target_path}")

    print()
    print(f"[SUCCESS] All {total_blocks} blocks applied to {len(units)} files.")

# --- Error Reporting ---


def print_error_report(errors: List[ValidationError]) -> None:
    print("[ERROR] Validation failed.")
    print()

    errors_by_file: Dict[str, List[ValidationError]] = {}
    for e in errors:
        errors_by_file.setdefault(e.file_path, []).append(e)

    for file_path in sorted(errors_by_file.keys()):
        print(f"File: {file_path}")
        file_errors = sorted(errors_by_file[file_path], key=lambda x: x.block_index)

        for err in file_errors:
            print(f"  - Block #{err.block_index}: {err.message}")
            if err.snippet:
                print(f'    Snippet: "{err.snippet}"')
        print()


# --- Main Execution ---


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        sys.stderr.write("Usage: fsrb <patch_file>\n")
        return 1

    patch_file_path = argv[1]
    if not os.path.exists(patch_file_path):
        sys.stderr.write(f"Error: Patch file '{patch_file_path}' not found.\n")
        return 1
    try:
        with open(patch_file_path, "r", encoding="utf-8", errors="strict", newline="") as f:
            patch_content = f.read()
    except Exception as e:
        sys.stderr.write(f"Error reading patch file: {e}\n")
        return 1

    # 1) Parse
    try:
        patch_blocks = parse_patch_file(patch_content)
    except Exception as e:
        sys.stderr.write(f"Error parsing patch file: {e}\n")
        return 1

    if not patch_blocks:
        sys.stderr.write("Warning: No valid patch blocks found in file.\n")
        return 0

    # 2) Validate (Dry Run with Memory Accumulation)
    file_states, errors = validate_and_apply_in_memory(patch_blocks)

    # 3) Report or Commit
    if errors:
        print_error_report(errors)
        return 1

    try:
        commit_changes_atomic(file_states, total_blocks=len(patch_blocks))
    except Exception as e:
        sys.stderr.write(f"CRITICAL ERROR: {e}\n")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
