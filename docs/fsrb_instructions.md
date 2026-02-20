# Task: Output a valid `.fsrb` patch file (Aider-editor-diff-compatible File Search/Replace Block)

**Format (repeat as needed for each file):**
path/to/file.ext
```
<<<<<<< SEARCH
<EXACT literal text copied from the target file>
=======
<new text to replace it with>
>>>>>>> REPLACE
```

**CRITICAL Rules:**
1. **Filename Format:**
   - The file path must be on its own line.
   - **NO backticks**, **NO bold**, **NO quotes**. Just the raw path.
   - Example: `src/main.rs` (GOOD), `src/main.rs` (BAD).

2. **Block Format:**
   - You SHOULD wrap the SEARCH/REPLACE blocks in Markdown code fences (```).
   - Use the exact delimiters: `<<<<<<< SEARCH`, `=======`, `>>>>>>> REPLACE`.

3. **Search Content:**
   - The SEARCH block must be an **exact literal match** of the file content.
   - Preserves all whitespace, indentation, and newlines exactly.
   - **Uniqueness:** The SEARCH block must match **exactly once** in the file. (0 matches = error, >1 matches = error).
   - If the search text is not unique, include more context lines until it is.

4. **Transactionality:**
   - The tool is **all-or-nothing**. If ANY block fails validation (e.g., text not found, ambiguous match, invalid create/delete), **NO files are modified**.

5. **Create / Delete:**
   - **CREATE file:** empty SEARCH + non-empty REPLACE. The target file must not exist; parent directories are created as needed.
   - **DELETE content / file:** non-empty SEARCH + empty REPLACE. If the final file becomes empty, the tool deletes the file; it may also remove now-empty parent directories (best-effort).

**Goal:** Produce valid `.fsrb` output where every SEARCH block matches the target file exactly once.
