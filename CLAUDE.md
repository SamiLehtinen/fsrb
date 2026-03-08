# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**fsrb** (File Search Replace Block) is a stateless utility that applies AI-generated code edits to files using exact literal string matching with all-or-nothing transactionality. Unlike `patch(1)`, it ignores line numbers and uses delimiter-based search/replace blocks (Aider-editor-diff-compatible format).

Three independent implementations exist: Rust (`src/fsrb.rs`), Python (`src/fsrb.py`), PowerShell (`src/fsrb.ps1`). All must behave identically.

## Running

```bash
# Python
python3 src/fsrb.py test/test.fsrb

# Rust (no Cargo.toml — compile directly)
rustc src/fsrb.rs -o fsrb && ./fsrb test/test.fsrb

# PowerShell
pwsh src/fsrb.ps1 test/test.fsrb
```

There is no build system, no external dependencies, and no test framework. The test file `test/test.fsrb` exercises a Create → Modify → Delete sequence. Run it in a temp directory to avoid modifying the working tree.

## Architecture

All implementations follow the same two-pass design:

1. **Parse**: Read the patch file into `PatchBlock` structs (file path, search content, replace content)
2. **Validate (dry run)**: Load all target files into memory, apply every block in-memory, collect all errors
3. **Commit (atomic)**: Write changes via temp file + rename, with rollback on any failure

Key invariants:
- Parse, validate, and commit are strictly separate phases
- SEARCH must match exactly 1 occurrence (0 = not found, >1 = ambiguous)
- Overlap-aware matching: "aa" in "aaa" counts as 2 matches
- Line endings normalized to LF for matching, original style preserved on write
- Temp files use PID + nanosecond timestamp to avoid collisions
- Deterministic output ordering (BTreeSet in Rust, sorted() in Python)

## Operations

- **UPDATE**: non-empty SEARCH + non-empty REPLACE
- **CREATE**: empty SEARCH + non-empty REPLACE (file must not exist; creates parent dirs)
- **DELETE**: non-empty SEARCH + empty REPLACE (removes file if content becomes empty; cleans empty parent dirs)

## Code Structure

Each implementation is a single self-contained file (~1000 lines) with no external dependencies:

- **Data types**: `PatchBlock`, `ValidationError`, `FileState`, `LineEnding`
- **Core functions**: `parse_patch_file()`, `validate_and_apply_in_memory()`, `commit_changes_atomic()`
- **Helpers**: line ending detection/conversion, overlap-aware match counting, error snippet extraction

## Spec Reference

`README.md` is the canonical specification. `docs/fsrb_instructions.md` contains the instructions format guide. When modifying behavior, ensure all three implementations stay in sync with each other and with the spec.
