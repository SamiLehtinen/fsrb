use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// --- Data Structures ---

#[derive(Debug, Clone)]
struct PatchBlock {
    file_path: String,
    /// Normalized to LF (`\n`) regardless of patch file line endings.
    search_pattern: String,
    /// Normalized to LF (`\n`) regardless of patch file line endings.
    replace_pattern: String,
    /// 1-based index of the block within the patch file
    block_index: usize,
    /// Line number in patch file where this block starts (for debugging)
    line_number: usize,
}

#[derive(Debug)]
struct ValidationError {
    file_path: String,
    block_index: usize,
    error_type: ValidationErrorType,
    message: String,
    snippet: String,
}

#[derive(Debug)]
enum ValidationErrorType {
    FileNotFound,
    FileUnreadable,
    EmptySearchBlock,
    SearchBlockNotFound,
    AmbiguousMatch,
    InvalidEmptySearchAndReplace,
    CreateOnExistingFile,
    CreateNotFirstBlock,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum LineEnding {
    Lf,
    Crlf,
}

struct FileState {
    path: String,
    /// Original line ending style detected from disk content.
    line_ending: LineEnding,
    /// Normalized file content using LF (`\n`) for matching/applying.
    content_norm: String,
    /// Whether the file existed on disk at validation time.
    existed_on_disk: bool,
    /// Whether it has been modified from the original disk state.
    modified: bool,
}

// --- Main Execution ---

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: fsrb <patch_file>");
        std::process::exit(1);
    }

    let patch_file_path = &args[1];
    if !Path::new(patch_file_path).exists() {
        eprintln!("Error: Patch file '{}' not found.", patch_file_path);
        std::process::exit(1);
    }

    let patch_content = match fs::read_to_string(patch_file_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading patch file: {}", e);
            std::process::exit(1);
        }
    };

    // 1) Parse
    let patch_blocks = match parse_patch_file(&patch_content) {
        Ok(blocks) => blocks,
        Err(e) => {
            eprintln!("Error parsing patch file: {}", e);
            std::process::exit(1);
        }
    };

    if patch_blocks.is_empty() {
        eprintln!("Warning: No valid patch blocks found in file.");
        std::process::exit(0);
    }

    // 2) Validate (Dry Run with Memory Accumulation)
    let (file_states, errors) = validate_and_apply_in_memory(&patch_blocks);

    // 3) Report or Commit
    if !errors.is_empty() {
        print_error_report(&errors);
        std::process::exit(1);
    }

    if let Err(e) = commit_changes_atomic(&file_states, patch_blocks.len()) {
        eprintln!("CRITICAL ERROR: {}", e);
        std::process::exit(1);
    }
}

// --- Parsing Logic ---

fn parse_patch_file(content: &str) -> Result<Vec<PatchBlock>, String> {
    let mut blocks = Vec::new();
    let lines: Vec<&str> = content.lines().collect();
    let mut i = 0;

    let mut current_file: Option<String> = None;
    let mut block_counter = 0;

    // Delimiters
    const DELIM_SEARCH: &str = "<<<<<<< SEARCH";
    const DELIM_SEP: &str = "=======";
    const DELIM_REPLACE: &str = ">>>>>>> REPLACE";

    fn is_ignored_line_for_header(t: &str) -> bool {
        let tt = t.trim();
        tt.is_empty() || tt.starts_with('#') || tt.starts_with("```")
    }

    fn unescape_delimiter_line(line: &str) -> &str {
        // Escape only recognized if "\" is the first character and the remainder is exactly a delimiter.
        if let Some(rest) = line.strip_prefix('\\') {
            if rest == DELIM_SEARCH || rest == DELIM_SEP || rest == DELIM_REPLACE {
                return rest;
            }
        }
        line
    }

    while i < lines.len() {
        let line = lines[i];

        // Skip markdown code fences (ignored as per spec)
        let trimmed = line.trim();
        if trimmed.starts_with("```") {
            i += 1;
            continue;
        }

        // Header format: a plain path line (e.g., "src/foo.py")
        // Only treat it as a file header if the next non-ignored line is a SEARCH block.
        let trimmed = line.trim();
        if !trimmed.is_empty()
            && trimmed != DELIM_SEARCH
            && trimmed != DELIM_SEP
            && trimmed != DELIM_REPLACE
            && !trimmed.starts_with('#')
        {
            let mut j = i + 1;
            while j < lines.len() && is_ignored_line_for_header(lines[j]) {
                j += 1;
            }
            if j < lines.len() && lines[j].trim() == DELIM_SEARCH {
                current_file = Some(trimmed.to_string());
                i += 1;
                continue;
            }
        }

        // Check for Search Block Start
        if line.trim() == DELIM_SEARCH {
            if current_file.is_none() {
                return Err(format!(
                    "Line {}: Found SEARCH block without a preceding file path header line.",
                    i + 1
                ));
            }

            block_counter += 1;
            let start_line = i + 1;
            i += 1;

            // Read SEARCH content (normalized to LF by join("\n")), with delimiter-line escaping support.
            let mut search_lines: Vec<String> = Vec::new();
            while i < lines.len() && lines[i].trim() != DELIM_SEP {
                search_lines.push(unescape_delimiter_line(lines[i]).to_string());
                i += 1;
            }

            if i >= lines.len() {
                return Err(format!(
                    "Block #{}: Unexpected end of file while reading SEARCH block.",
                    block_counter
                ));
            }
            // Skip the separator "======="
            i += 1;

            // Read REPLACE content (normalized to LF by join("\n")), with delimiter-line escaping support.
            let mut replace_lines: Vec<String> = Vec::new();
            while i < lines.len() && lines[i].trim() != DELIM_REPLACE {
                replace_lines.push(unescape_delimiter_line(lines[i]).to_string());
                i += 1;
            }

            if i >= lines.len() {
                return Err(format!(
                    "Block #{}: Unexpected end of file while reading REPLACE block.",
                    block_counter
                ));
            }
            // Skip the end marker ">>>>>>> REPLACE"
            i += 1;

            // Normalize to LF explicitly (join uses "\n"; patch file CRLF is already handled by lines()).
            let search = normalize_to_lf(&search_lines.join("\n"));
            let replace = normalize_to_lf(&replace_lines.join("\n"));

            blocks.push(PatchBlock {
                file_path: current_file.as_ref().unwrap().clone(),
                search_pattern: search,
                replace_pattern: replace,
                block_index: block_counter,
                line_number: start_line,
            });
        } else {
            // Ignore other lines (comments, empty lines between blocks)
            i += 1;
        }
    }

    Ok(blocks)
}

// --- Validation & In-Memory Application ---

fn validate_and_apply_in_memory(blocks: &[PatchBlock]) -> (HashMap<String, FileState>, Vec<ValidationError>) {
    let mut file_states: HashMap<String, FileState> = HashMap::new();
    let mut errors: Vec<ValidationError> = Vec::new();

    // Pre-collect unique file paths (deterministic order) and load once.
    let mut unique_paths: BTreeSet<String> = BTreeSet::new();
    for b in blocks {
        unique_paths.insert(b.file_path.clone());
    }

    // Map file -> first block index referencing it (for consistent file-level load errors).
    let mut first_block_for_file: HashMap<String, usize> = HashMap::new();
    for b in blocks {
        first_block_for_file
            .entry(b.file_path.clone())
            .and_modify(|cur| {
                if b.block_index < *cur {
                    *cur = b.block_index;
                }
            })
            .or_insert(b.block_index);
    }

    // Pre-scan which files are intended to be created.
    let mut create_files: BTreeSet<String> = BTreeSet::new();
    for b in blocks {
        if b.search_pattern.is_empty() && !b.replace_pattern.is_empty() {
            create_files.insert(b.file_path.clone());
        }
    }

    // Track unavailable files to skip subsequent block processing without spamming errors.
    let mut unavailable_files: BTreeSet<String> = BTreeSet::new();

    // Load all referenced files (or create empty in-memory states for CREATE targets).
    for path in &unique_paths {
        let p = Path::new(path);
        if p.exists() {
            match fs::read_to_string(p) {
                Ok(content) => {
                    let line_ending = detect_line_ending(&content);
                    let content_norm = normalize_to_lf(&content);
                    file_states.insert(
                        path.clone(),
                        FileState {
                            path: path.clone(),
                            line_ending,
                            content_norm,
                            existed_on_disk: true,
                            modified: false,
                        },
                    );
                }
                Err(e) => {
                    errors.push(ValidationError {
                        file_path: path.clone(),
                        block_index: *first_block_for_file.get(path).unwrap_or(&1),
                        error_type: ValidationErrorType::FileUnreadable,
                        message: format!("Error reading file: {}", e),
                        snippet: String::new(),
                    });
                    unavailable_files.insert(path.clone());
                }
            }
        } else {
            if create_files.contains(path) {
                // CREATE targets start from an empty, non-existent state.
                file_states.insert(
                    path.clone(),
                    FileState {
                        path: path.clone(),
                        line_ending: LineEnding::Lf,
                        content_norm: String::new(),
                        existed_on_disk: false,
                        modified: false,
                    },
                );
            } else {
                errors.push(ValidationError {
                    file_path: path.clone(),
                    block_index: *first_block_for_file.get(path).unwrap_or(&1),
                    error_type: ValidationErrorType::FileNotFound,
                    message: "File not found on disk.".to_string(),
                    snippet: String::new(),
                });
                unavailable_files.insert(path.clone());
            }
        }
    }

    // Track whether a CREATE has already been applied per file, and enforce ordering.
    let mut seen_noncreate_for_file: HashMap<String, bool> = HashMap::new();
    let mut seen_create_for_file: HashMap<String, bool> = HashMap::new();

    // Apply blocks to the current in-memory (normalized) state, accumulating all errors.
    for block in blocks {
        if unavailable_files.contains(&block.file_path) {
            continue;
        }

        let Some(state) = file_states.get_mut(&block.file_path) else {
            // Should not happen if preloading succeeded, but keep robust.
            errors.push(ValidationError {
                file_path: block.file_path.clone(),
                block_index: block.block_index,
                error_type: ValidationErrorType::FileUnreadable,
                message: "Internal error: file state not loaded.".to_string(),
                snippet: String::new(),
            });
            continue;
        };

        let is_create = block.search_pattern.is_empty() && !block.replace_pattern.is_empty();
        let is_invalid_empty = block.search_pattern.is_empty() && block.replace_pattern.is_empty();

        if is_invalid_empty {
            errors.push(ValidationError {
                file_path: block.file_path.clone(),
                block_index: block.block_index,
                error_type: ValidationErrorType::InvalidEmptySearchAndReplace,
                message: "Invalid block: SEARCH and REPLACE are both empty.".to_string(),
                snippet: String::new(),
            });
            continue;
        }

        if is_create {
            if state.existed_on_disk {
                errors.push(ValidationError {
                    file_path: block.file_path.clone(),
                    block_index: block.block_index,
                    error_type: ValidationErrorType::CreateOnExistingFile,
                    message: "CREATE block is only valid when the target file does not exist.".to_string(),
                    snippet: String::new(),
                });
                continue;
            }

            if *seen_noncreate_for_file.get(&block.file_path).unwrap_or(&false) {
                errors.push(ValidationError {
                    file_path: block.file_path.clone(),
                    block_index: block.block_index,
                    error_type: ValidationErrorType::CreateNotFirstBlock,
                    message: "CREATE block must be the first block for its file.".to_string(),
                    snippet: String::new(),
                });
                continue;
            }

            if *seen_create_for_file.get(&block.file_path).unwrap_or(&false) {
                errors.push(ValidationError {
                    file_path: block.file_path.clone(),
                    block_index: block.block_index,
                    error_type: ValidationErrorType::AmbiguousMatch,
                    message: "Multiple CREATE blocks for the same file are not allowed.".to_string(),
                    snippet: String::new(),
                });
                continue;
            }

            // Apply CREATE by setting the entire file content from empty.
            state.content_norm = block.replace_pattern.clone();
            state.modified = true;
            seen_create_for_file.insert(block.file_path.clone(), true);
            continue;
        }

        // Non-CREATE blocks: enforce legacy rule that SEARCH must not be empty.
        if block.search_pattern.is_empty() {
            errors.push(ValidationError {
                file_path: block.file_path.clone(),
                block_index: block.block_index,
                error_type: ValidationErrorType::EmptySearchBlock,
                message: "SEARCH block is empty.".to_string(),
                snippet: String::new(),
            });
            continue;
        }

        seen_noncreate_for_file.insert(block.file_path.clone(), true);

        // Occurrence count must be overlap-aware to avoid false uniqueness.
        match find_unique_overlapping(&state.content_norm, &block.search_pattern) {
            UniqueMatch::None => {
                errors.push(ValidationError {
                    file_path: block.file_path.clone(),
                    block_index: block.block_index,
                    error_type: ValidationErrorType::SearchBlockNotFound,
                    message: "SEARCH block not found in file content.".to_string(),
                    snippet: get_snippet(&block.search_pattern),
                });
            }
            UniqueMatch::Ambiguous { count } => {
                errors.push(ValidationError {
                    file_path: block.file_path.clone(),
                    block_index: block.block_index,
                    error_type: ValidationErrorType::AmbiguousMatch,
                    message: format!("Ambiguous match. Found {} occurrences.", count),
                    snippet: get_snippet(&block.search_pattern),
                });
            }
            UniqueMatch::One { start } => {
                // Apply exactly once at the unique position.
                state.content_norm = replace_at(&state.content_norm, start, block.search_pattern.len(), &block.replace_pattern);
                state.modified = true;
            }
        }
    }

    (file_states, errors)
}

// --- Transactional Commit (All-or-Nothing, Atomic) ---

fn ensure_parent_dirs_for_create(target_path: &Path) -> io::Result<Vec<PathBuf>> {
    let mut actual_created: Vec<PathBuf> = Vec::new();
    if let Some(parent) = target_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            // Identify the first existing ancestor, collecting missing dirs.
            let mut p = parent;
            let mut stack = Vec::new();
            while !p.exists() && !p.as_os_str().is_empty() {
                stack.push(p.to_path_buf());
                if let Some(par) = p.parent() {
                    p = par;
                } else {
                    break;
                }
            }
            // Create them from top (shallowest) to bottom (deepest).
            while let Some(dir) = stack.pop() {
                fs::create_dir(&dir)?;
                actual_created.push(dir);
            }
        }
    }
    // Return in reverse order (bottom-up / deepest-first) for easier removal.
    actual_created.reverse();
    Ok(actual_created)
}

fn remove_empty_dirs_best_effort(dir_list: &[PathBuf]) {
    for d in dir_list {
        let _ = fs::remove_dir(d); // fails if not empty
    }
}

fn remove_empty_parent_dirs_best_effort(file_path: &Path) {
    if let Some(mut parent) = file_path.parent() {
        while !parent.as_os_str().is_empty() {
            if fs::remove_dir(parent).is_err() {
                // Stop if we can't remove it (e.g. not empty, or permission error)
                break;
            }
            if let Some(p) = parent.parent() {
                parent = p;
            } else {
                break;
            }
        }
    }
}

fn commit_changes_atomic(file_states: &HashMap<String, FileState>, total_blocks: usize) -> io::Result<()> {
    // Collect modified files in deterministic path order.
    let mut paths: Vec<&String> = file_states.keys().collect();
    paths.sort();

    let mut units: Vec<CommitUnit> = Vec::new();
    for p in paths {
        let st = &file_states[p];
        if !st.modified {
            continue;
        }

        // Logic:
        // Create: if not existed_on_disk and content != ""
        // Delete: if existed_on_disk and content is empty/whitespace
        // Update: if existed_on_disk and content is not empty

        let is_empty_content = st.content_norm.trim().is_empty();

        let action = if !st.existed_on_disk && !is_empty_content {
            "create"
        } else if st.existed_on_disk && is_empty_content {
            "delete"
        } else if st.existed_on_disk && !is_empty_content {
            "update"
        } else {
            // e.g., created then deleted within the same transaction => no-op on disk
            continue;
        };

        let final_content = match st.line_ending {
            LineEnding::Lf => st.content_norm.clone(),
            LineEnding::Crlf => lf_to_crlf(&st.content_norm),
        };

        units.push(CommitUnit {
            target_path: PathBuf::from(&st.path),
            final_content,
            action: action.to_string(),
            temp_path: None,
            backup_path: None,
        });
    }

    // If nothing changed, still emit success with 0 files.
    if units.is_empty() {
        println!();
        println!("[SUCCESS] All {} blocks applied to 0 files.", total_blocks);
        return Ok(());
    }

    // Phase A: create any needed parent dirs for CREATE targets, then write temp files.
    let mut created_dirs: Vec<PathBuf> = Vec::new();
    for unit in &mut units {
        if unit.action == "create" {
            created_dirs.extend(ensure_parent_dirs_for_create(&unit.target_path)?);
        }

        if unit.action == "create" || unit.action == "update" {
            let target = &unit.target_path;

            // Ensure parent exists.
            if let Some(parent) = target.parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Target directory does not exist: {}", parent.display()),
                    ));
                }
            }

            let temp = unique_sibling_path(target, ".fsrb.tmp")?;
            let mut f = create_new_file(&temp)?;
            f.write_all(unit.final_content.as_bytes())?;
            f.flush()?;
            f.sync_all()?; // durability for temp content

            // Preserve permissions as best-effort (important on Unix); ignore if fails.
            if unit.action == "update" {
                if let Ok(meta) = fs::metadata(target) {
                    let _ = fs::set_permissions(&temp, meta.permissions());
                }
            }

            unit.temp_path = Some(temp);
        }
    }

    // Phase B: swap in atomically per file with rollback on any failure.
    let mut moved_originals: Vec<PathBuf> = Vec::new(); // targets that have been renamed to backups
    let mut installed_news: Vec<PathBuf> = Vec::new(); // targets where new content has been installed

    let swap_result: io::Result<()> = (|| {
        for unit in &mut units {
            let target = &unit.target_path;

            if unit.action == "create" {
                let temp = unit
                    .temp_path
                    .as_ref()
                    .expect("temp_path must exist before swapping");

                // Ensure target does not exist (strict CREATE semantics).
                if target.exists() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("CREATE target already exists: {}", target.display()),
                    ));
                }

                // Move temp into place (atomic). Target does not exist at this moment.
                fs::rename(temp, target)?;
                installed_news.push(target.clone());

                // Best-effort sync parent dir for rename durability (platform dependent).
                let _ = sync_parent_dir(target);
                continue;
            }

            if unit.action == "delete" {
                // Move original out of the way; do not install a new file.
                if !target.exists() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("DELETE target missing at commit time: {}", target.display()),
                    ));
                }

                let backup = unique_sibling_path(target, ".fsrb.bak")?;
                unit.backup_path = Some(backup.clone());

                // Move original out of the way (atomic).
                fs::rename(target, &backup)?;
                moved_originals.push(target.clone());

                // Best-effort sync parent dir for rename durability (platform dependent).
                let _ = sync_parent_dir(target);
                continue;
            }

            // update
            let temp = unit
                .temp_path
                .as_ref()
                .expect("temp_path must exist before swapping");

            let backup = unique_sibling_path(target, ".fsrb.bak")?;
            unit.backup_path = Some(backup.clone());

            // Move original out of the way (atomic).
            fs::rename(target, &backup)?;
            moved_originals.push(target.clone());

            // Move temp into place (atomic). Target does not exist at this moment.
            fs::rename(temp, target)?;
            installed_news.push(target.clone());

            // Best-effort sync parent dir for rename durability (platform dependent).
            let _ = sync_parent_dir(target);
        }
        Ok(())
    })();

    if let Err(e) = swap_result {
        // Rollback: restore originals for any swapped file, and restore moved originals that haven't had new content installed.
        let rollback_err = rollback_swaps(&units, &installed_news, &moved_originals);

        // Always try to clean up temps/backups after rollback attempt.
        let _ = cleanup_leftovers(&units);

        // Remove created directories
        remove_empty_dirs_best_effort(&created_dirs);

        if let Some(rbe) = rollback_err {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Commit failed: {}. Rollback also encountered errors: {}", e, rbe),
            ));
        }
        return Err(e);
    }

    // Phase C: delete backups and any leftover temps.
    cleanup_leftovers(&units)?;

    // Post-commit: remove empty parent dirs for DELETE (best-effort).
    for unit in &units {
        if unit.action == "delete" && unit.backup_path.is_some() {
            remove_empty_parent_dirs_best_effort(&unit.target_path);
        }
    }

    // Success output (deterministic token); line-per-file lines are stable due to sorting.
    for unit in &units {
        println!("Saved changes to {}", unit.target_path.display());
    }

    println!();
    println!("[SUCCESS] All {} blocks applied to {} files.", total_blocks, units.len());

    Ok(())
}

struct CommitUnit {
    target_path: PathBuf,
    final_content: String,
    action: String,  // "update" | "create" | "delete"
    temp_path: Option<PathBuf>,
    backup_path: Option<PathBuf>,
}

fn rollback_swaps(
    units: &[CommitUnit],
    installed_news: &[PathBuf],
    moved_originals: &[PathBuf],
) -> Option<String> {
    let mut errs: Vec<String> = Vec::new();

    // 1) For targets with new installed: restore backup over it (reverse install order).
    for target in installed_news.iter().rev() {
        let unit = units.iter().find(|u| &u.target_path == target);
        let Some(unit) = unit else { continue };

        // CREATE has no backup: rollback must remove the created target file.
        if unit.action == "create" {
            if target.exists() {
                if let Err(e) = fs::remove_file(target) {
                    errs.push(format!(
                        "Rollback: failed to remove created file {}: {}",
                        target.display(),
                        e
                    ));
                } else {
                    let _ = sync_parent_dir(target);
                }
            }
            continue;
        }

        // UPDATE: restore backup over the installed new file.
        let Some(backup) = unit.backup_path.as_ref() else { continue };

        match unique_sibling_path(target, ".fsrb.rollback") {
            Ok(roll_tmp) => {
                if let Err(e) = fs::rename(target, &roll_tmp) {
                    errs.push(format!(
                        "Rollback: failed to move new target aside ({}): {}",
                        target.display(),
                        e
                    ));
                    continue;
                }

                if let Err(e) = fs::rename(backup, target) {
                    errs.push(format!(
                        "Rollback: failed to restore backup ({} -> {}): {}",
                        backup.display(),
                        target.display(),
                        e
                    ));
                    // Try to put new content back to avoid leaving missing target.
                    let _ = fs::rename(&roll_tmp, target);
                    continue;
                }

                let _ = fs::remove_file(&roll_tmp);
                let _ = sync_parent_dir(target);
            }
            Err(e) => errs.push(format!(
                "Rollback: failed to create rollback temp path for {}: {}",
                target.display(),
                e
            )),
        }
    }

    // 2) For originals that were moved but might not have had new installed (DELETE):
    // If target doesn't exist and backup exists, restore backup.
    for target in moved_originals.iter().rev() {
        let unit = units.iter().find(|u| &u.target_path == target);
        let Some(unit) = unit else { continue };
        let Some(backup) = unit.backup_path.as_ref() else { continue };

        if target.exists() {
            continue;
        }
        if backup.exists() {
            if let Err(e) = fs::rename(backup, target) {
                errs.push(format!(
                    "Rollback: failed to restore moved original ({} -> {}): {}",
                    backup.display(),
                    target.display(),
                    e
                ));
            } else {
                let _ = sync_parent_dir(target);
            }
        }
    }

    if errs.is_empty() {
        None
    } else {
        Some(errs.join(" | "))
    }
}

fn cleanup_leftovers(units: &[CommitUnit]) -> io::Result<()> {
    let mut first_err: Option<io::Error> = None;

    for u in units {
        if let Some(tmp) = &u.temp_path {
            // temp might have been renamed into place; ignore errors.
            let _ = fs::remove_file(tmp);
        }
        if let Some(bak) = &u.backup_path {
            // remove backups after successful commit; ignore if already moved back/removed.
            let _ = fs::remove_file(bak);
        }
    }

    if let Some(e) = first_err.take() {
        Err(e)
    } else {
        Ok(())
    }
}

// --- Helpers: Matching / Replacement / Line Endings ---

fn detect_line_ending(content: &str) -> LineEnding {
    if content.contains("\r\n") {
        LineEnding::Crlf
    } else {
        LineEnding::Lf
    }
}

fn normalize_to_lf(s: &str) -> String {
    // Convert CRLF -> LF. Leave lone CR untouched (rare, but avoid destructive transforms).
    s.replace("\r\n", "\n")
}

fn lf_to_crlf(s: &str) -> String {
    // Convert LF -> CRLF. Assumes input does not contain CRLF already.
    s.replace('\n', "\r\n")
}

enum UniqueMatch {
    None,
    One { start: usize },
    Ambiguous { count: usize },
}

/// Overlap-aware substring uniqueness check.
/// Returns unique start index if exactly one occurrence exists.
fn find_unique_overlapping(haystack: &str, needle: &str) -> UniqueMatch {
    let hb = haystack.as_bytes();
    let nb = needle.as_bytes();

    if nb.is_empty() || nb.len() > hb.len() {
        return UniqueMatch::None;
    }

    let mut count = 0usize;
    let mut first_pos: usize = 0;

    // Overlap-aware scan with an exact occurrence count.
    for i in 0..=hb.len() - nb.len() {
        if &hb[i..i + nb.len()] == nb {
            count += 1;
            if count == 1 {
                first_pos = i;
            }
        }
    }

    match count {
        0 => UniqueMatch::None,
        1 => UniqueMatch::One { start: first_pos },
        _ => UniqueMatch::Ambiguous { count },
    }
}

fn replace_at(haystack: &str, start: usize, needle_len: usize, replacement: &str) -> String {
    let mut out = String::with_capacity(haystack.len().saturating_sub(needle_len) + replacement.len());
    out.push_str(&haystack[..start]);
    out.push_str(replacement);
    out.push_str(&haystack[start + needle_len..]);
    out
}

fn get_snippet(text: &str) -> String {
    let first_line = text.lines().next().unwrap_or("");
    if first_line.len() > 60 {
        format!("{}...", &first_line[0..60])
    } else {
        first_line.to_string()
    }
}

// --- Helpers: Temp/Backup Paths and I/O ---

fn unique_sibling_path(target: &Path, tag: &str) -> io::Result<PathBuf> {
    let parent = target.parent().unwrap_or_else(|| Path::new(""));
    let file_name = target
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Target path has no file name"))?
        .to_string_lossy();

    let pid = std::process::id();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    for n in 0u32..10_000 {
        let candidate = parent.join(format!("{}.{}.{pid}.{now}.{n}", file_name, tag));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "Unable to allocate unique temp path for {}",
            target.display()
        ),
    ))
}

fn create_new_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}

fn sync_parent_dir(target: &Path) -> io::Result<()> {
    let Some(parent) = target.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    // Directory fsync is best-effort; may fail on some platforms/filesystems.
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

// --- Error Reporting ---

fn print_error_report(errors: &[ValidationError]) {
    // Deterministic failure token for agent parsing.
    println!("[ERROR] Validation failed.");
    println!();

    // Group by file; deterministic order by file name, then block index.
    let mut errors_by_file: HashMap<String, Vec<&ValidationError>> = HashMap::new();
    for error in errors {
        errors_by_file
            .entry(error.file_path.clone())
            .or_insert_with(Vec::new)
            .push(error);
    }

    let mut sorted_files: Vec<_> = errors_by_file.keys().cloned().collect();
    sorted_files.sort();

    for file_path in sorted_files {
        println!("File: {}", file_path);
        let mut file_errors = errors_by_file.get(&file_path).unwrap().clone();
        file_errors.sort_by_key(|e| e.block_index);

        for error in file_errors {
            let msg = match error.error_type {
                ValidationErrorType::FileNotFound
                | ValidationErrorType::FileUnreadable
                | ValidationErrorType::EmptySearchBlock
                | ValidationErrorType::SearchBlockNotFound
                | ValidationErrorType::AmbiguousMatch
                | ValidationErrorType::InvalidEmptySearchAndReplace
                | ValidationErrorType::CreateOnExistingFile
                | ValidationErrorType::CreateNotFirstBlock => error.message.clone(),
            };

            println!("  - Block #{}: {}", error.block_index, msg);
            if !error.snippet.is_empty() {
                println!("    Snippet: \"{}\"", error.snippet);
            }
        }
        println!();
    }
}

// --- Unit Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn create_temp_dir() -> PathBuf {
        let n = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "fsrb_test_{}_{}_{}",
            std::process::id(),
            n,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup_temp_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    fn make_block(file_path: &str, search: &str, replace: &str, index: usize) -> PatchBlock {
        PatchBlock {
            file_path: file_path.to_string(),
            search_pattern: search.to_string(),
            replace_pattern: replace.to_string(),
            block_index: index,
            line_number: 1,
        }
    }

    // ==================== Line Ending Helpers ====================

    #[test]
    fn test_detect_line_ending_lf() {
        assert_eq!(detect_line_ending("hello\nworld\n"), LineEnding::Lf);
    }

    #[test]
    fn test_detect_line_ending_crlf() {
        assert_eq!(detect_line_ending("hello\r\nworld\r\n"), LineEnding::Crlf);
    }

    #[test]
    fn test_detect_line_ending_no_newlines() {
        assert_eq!(detect_line_ending("hello"), LineEnding::Lf);
    }

    #[test]
    fn test_detect_line_ending_empty() {
        assert_eq!(detect_line_ending(""), LineEnding::Lf);
    }

    #[test]
    fn test_detect_line_ending_mixed_prefers_crlf() {
        // If any CRLF exists, detect as CRLF
        assert_eq!(detect_line_ending("a\nb\r\nc\n"), LineEnding::Crlf);
    }

    #[test]
    fn test_normalize_to_lf() {
        assert_eq!(normalize_to_lf("a\r\nb\r\n"), "a\nb\n");
        assert_eq!(normalize_to_lf("a\nb\n"), "a\nb\n");
        assert_eq!(normalize_to_lf("no newlines"), "no newlines");
        assert_eq!(normalize_to_lf(""), "");
    }

    #[test]
    fn test_normalize_to_lf_lone_cr_preserved() {
        // Lone \r should not be modified
        assert_eq!(normalize_to_lf("a\rb"), "a\rb");
    }

    #[test]
    fn test_lf_to_crlf() {
        assert_eq!(lf_to_crlf("a\nb\n"), "a\r\nb\r\n");
        assert_eq!(lf_to_crlf("no newlines"), "no newlines");
        assert_eq!(lf_to_crlf(""), "");
    }

    // ==================== Overlap-Aware Matching ====================

    #[test]
    fn test_find_unique_overlapping_one_match() {
        match find_unique_overlapping("hello world", "world") {
            UniqueMatch::One { start } => assert_eq!(start, 6),
            _ => panic!("Expected exactly one match"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_no_match() {
        match find_unique_overlapping("hello world", "xyz") {
            UniqueMatch::None => {}
            _ => panic!("Expected no match"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_ambiguous() {
        match find_unique_overlapping("abab", "ab") {
            UniqueMatch::Ambiguous { count } => assert_eq!(count, 2),
            _ => panic!("Expected ambiguous match"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_overlap_counting() {
        match find_unique_overlapping("aaa", "aa") {
            UniqueMatch::Ambiguous { count } => assert_eq!(count, 2),
            _ => panic!("Expected 2 overlapping occurrences"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_empty_needle() {
        match find_unique_overlapping("hello", "") {
            UniqueMatch::None => {}
            _ => panic!("Empty needle should return None"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_needle_longer_than_haystack() {
        match find_unique_overlapping("hi", "hello world") {
            UniqueMatch::None => {}
            _ => panic!("Needle longer than haystack should return None"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_exact_match() {
        match find_unique_overlapping("hello", "hello") {
            UniqueMatch::One { start } => assert_eq!(start, 0),
            _ => panic!("Expected one match at start"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_single_char() {
        match find_unique_overlapping("abcde", "c") {
            UniqueMatch::One { start } => assert_eq!(start, 2),
            _ => panic!("Expected one match"),
        }
    }

    #[test]
    fn test_find_unique_overlapping_three_occurrences() {
        match find_unique_overlapping("abcabcabc", "abc") {
            UniqueMatch::Ambiguous { count } => assert_eq!(count, 3),
            _ => panic!("Expected 3 occurrences"),
        }
    }

    // ==================== replace_at ====================

    #[test]
    fn test_replace_at_basic() {
        assert_eq!(replace_at("hello world", 6, 5, "rust"), "hello rust");
    }

    #[test]
    fn test_replace_at_beginning() {
        assert_eq!(replace_at("hello world", 0, 5, "goodbye"), "goodbye world");
    }

    #[test]
    fn test_replace_at_end() {
        assert_eq!(replace_at("hello world", 6, 5, ""), "hello ");
    }

    #[test]
    fn test_replace_at_empty_replacement() {
        assert_eq!(replace_at("abcde", 1, 3, ""), "ae");
    }

    #[test]
    fn test_replace_at_larger_replacement() {
        assert_eq!(replace_at("ab", 1, 1, "xyz"), "axyz");
    }

    #[test]
    fn test_replace_at_full_string() {
        assert_eq!(replace_at("hello", 0, 5, "world"), "world");
    }

    // ==================== get_snippet ====================

    #[test]
    fn test_get_snippet_short() {
        assert_eq!(get_snippet("short line"), "short line");
    }

    #[test]
    fn test_get_snippet_truncates_long_line() {
        let long = "a".repeat(100);
        let snip = get_snippet(&long);
        assert_eq!(snip.len(), 63); // 60 + "..."
        assert!(snip.ends_with("..."));
    }

    #[test]
    fn test_get_snippet_multiline_uses_first() {
        assert_eq!(get_snippet("first\nsecond\nthird"), "first");
    }

    #[test]
    fn test_get_snippet_empty() {
        assert_eq!(get_snippet(""), "");
    }

    #[test]
    fn test_get_snippet_exactly_60_chars() {
        let s = "a".repeat(60);
        assert_eq!(get_snippet(&s), s);
    }

    #[test]
    fn test_get_snippet_61_chars_truncated() {
        let s = "a".repeat(61);
        let snip = get_snippet(&s);
        assert!(snip.ends_with("..."));
        assert_eq!(snip.len(), 63);
    }

    // ==================== Parsing ====================

    #[test]
    fn test_parse_single_block() {
        let input = "\
src/main.rs
<<<<<<< SEARCH
old code
=======
new code
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].file_path, "src/main.rs");
        assert_eq!(blocks[0].search_pattern, "old code");
        assert_eq!(blocks[0].replace_pattern, "new code");
        assert_eq!(blocks[0].block_index, 1);
    }

    #[test]
    fn test_parse_multiple_blocks_same_file() {
        let input = "\
src/main.rs
<<<<<<< SEARCH
aaa
=======
bbb
>>>>>>> REPLACE

src/main.rs
<<<<<<< SEARCH
ccc
=======
ddd
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].search_pattern, "aaa");
        assert_eq!(blocks[1].search_pattern, "ccc");
        assert_eq!(blocks[0].block_index, 1);
        assert_eq!(blocks[1].block_index, 2);
    }

    #[test]
    fn test_parse_multiple_files() {
        let input = "\
file_a.txt
<<<<<<< SEARCH
old_a
=======
new_a
>>>>>>> REPLACE

file_b.txt
<<<<<<< SEARCH
old_b
=======
new_b
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].file_path, "file_a.txt");
        assert_eq!(blocks[1].file_path, "file_b.txt");
    }

    #[test]
    fn test_parse_create_block() {
        let input = "\
new_file.txt
<<<<<<< SEARCH
=======
file content here
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].search_pattern, "");
        assert_eq!(blocks[0].replace_pattern, "file content here");
    }

    #[test]
    fn test_parse_delete_block() {
        let input = "\
old_file.txt
<<<<<<< SEARCH
content to remove
=======
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].search_pattern, "content to remove");
        assert_eq!(blocks[0].replace_pattern, "");
    }

    #[test]
    fn test_parse_comments_ignored() {
        let input = "\
# This is a comment
src/main.rs
<<<<<<< SEARCH
old
=======
new
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].file_path, "src/main.rs");
    }

    #[test]
    fn test_parse_code_fences_ignored() {
        let input = "\
```
src/main.rs
<<<<<<< SEARCH
old
=======
new
>>>>>>> REPLACE
```
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].file_path, "src/main.rs");
    }

    #[test]
    fn test_parse_delimiter_escaping() {
        let input = "\
src/main.rs
<<<<<<< SEARCH
before
\\<<<<<<< SEARCH
after
=======
before
\\=======
after
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks[0].search_pattern, "before\n<<<<<<< SEARCH\nafter");
        assert_eq!(blocks[0].replace_pattern, "before\n=======\nafter");
    }

    #[test]
    fn test_parse_escape_replace_delimiter() {
        let input = "\
file.txt
<<<<<<< SEARCH
old
=======
\\>>>>>>> REPLACE
more content
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks[0].replace_pattern, ">>>>>>> REPLACE\nmore content");
    }

    #[test]
    fn test_parse_multiline_content() {
        let input = "\
file.txt
<<<<<<< SEARCH
line 1
line 2
line 3
=======
new line 1
new line 2
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks[0].search_pattern, "line 1\nline 2\nline 3");
        assert_eq!(blocks[0].replace_pattern, "new line 1\nnew line 2");
    }

    #[test]
    fn test_parse_error_search_without_header() {
        let input = "\
<<<<<<< SEARCH
old
=======
new
>>>>>>> REPLACE
";
        assert!(parse_patch_file(input).is_err());
    }

    #[test]
    fn test_parse_error_unterminated_search() {
        let input = "\
file.txt
<<<<<<< SEARCH
old content
";
        assert!(parse_patch_file(input).is_err());
    }

    #[test]
    fn test_parse_error_unterminated_replace() {
        let input = "\
file.txt
<<<<<<< SEARCH
old
=======
new content
";
        assert!(parse_patch_file(input).is_err());
    }

    #[test]
    fn test_parse_empty_input() {
        let blocks = parse_patch_file("").unwrap();
        assert!(blocks.is_empty());
    }

    #[test]
    fn test_parse_whitespace_trimmed_delimiters() {
        let input = "\
file.txt
  <<<<<<< SEARCH
old
  =======
new
  >>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].search_pattern, "old");
        assert_eq!(blocks[0].replace_pattern, "new");
    }

    #[test]
    fn test_parse_blank_lines_between_header_and_search() {
        let input = "\
file.txt

<<<<<<< SEARCH
old
=======
new
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].file_path, "file.txt");
    }

    #[test]
    fn test_parse_only_comments() {
        let input = "# just a comment\n# another comment\n";
        let blocks = parse_patch_file(input).unwrap();
        assert!(blocks.is_empty());
    }

    #[test]
    fn test_parse_backslash_not_before_delimiter() {
        // Backslash before non-delimiter should be preserved
        let input = "\
file.txt
<<<<<<< SEARCH
\\not_a_delimiter
=======
new
>>>>>>> REPLACE
";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks[0].search_pattern, "\\not_a_delimiter");
    }

    #[test]
    fn test_parse_crlf_patch_file() {
        let input = "file.txt\r\n<<<<<<< SEARCH\r\nold\r\n=======\r\nnew\r\n>>>>>>> REPLACE\r\n";
        let blocks = parse_patch_file(input).unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].search_pattern, "old");
        assert_eq!(blocks[0].replace_pattern, "new");
    }

    #[test]
    fn test_parse_file_header_not_recognized_without_search() {
        // A line that looks like a path but not followed by SEARCH should be ignored
        let input = "some/path.txt\nrandom text\n";
        let blocks = parse_patch_file(input).unwrap();
        assert!(blocks.is_empty());
    }

    // ==================== Validation & In-Memory Application ====================

    #[test]
    fn test_validate_simple_update() {
        let tmp = create_temp_dir();
        let file = tmp.join("update.txt");
        fs::write(&file, "hello world").unwrap();

        let blocks = vec![make_block(&file.to_string_lossy(), "hello", "goodbye", 1)];
        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
        assert_eq!(states[&file.to_string_lossy().to_string()].content_norm, "goodbye world");
        assert!(states[&file.to_string_lossy().to_string()].modified);

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_search_not_found() {
        let tmp = create_temp_dir();
        let file = tmp.join("notfound.txt");
        fs::write(&file, "hello world").unwrap();

        let blocks = vec![make_block(&file.to_string_lossy(), "nonexistent", "x", 1)];
        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].error_type, ValidationErrorType::SearchBlockNotFound));

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_ambiguous_match() {
        let tmp = create_temp_dir();
        let file = tmp.join("ambiguous.txt");
        fs::write(&file, "foo bar foo baz").unwrap();

        let blocks = vec![make_block(&file.to_string_lossy(), "foo", "xxx", 1)];
        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].error_type, ValidationErrorType::AmbiguousMatch));

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_file_not_found() {
        let blocks = vec![make_block("/tmp/fsrb_nonexistent_file_xyzzy.txt", "hello", "world", 1)];
        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].error_type, ValidationErrorType::FileNotFound));
    }

    #[test]
    fn test_validate_create_operation() {
        let tmp = create_temp_dir();
        let file = tmp.join("brand_new.txt");

        let blocks = vec![make_block(&file.to_string_lossy(), "", "new content", 1)];
        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
        let state = &states[&file.to_string_lossy().to_string()];
        assert_eq!(state.content_norm, "new content");
        assert!(!state.existed_on_disk);
        assert!(state.modified);

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_create_on_existing_file() {
        let tmp = create_temp_dir();
        let file = tmp.join("exists.txt");
        fs::write(&file, "existing content").unwrap();

        let blocks = vec![make_block(&file.to_string_lossy(), "", "new content", 1)];
        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].error_type, ValidationErrorType::CreateOnExistingFile));

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_empty_search_and_replace() {
        let tmp = create_temp_dir();
        let file = tmp.join("empty.txt");
        fs::write(&file, "content").unwrap();

        let blocks = vec![make_block(&file.to_string_lossy(), "", "", 1)];
        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].error_type, ValidationErrorType::InvalidEmptySearchAndReplace));

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_multiple_blocks_sequential() {
        let tmp = create_temp_dir();
        let file = tmp.join("multi.txt");
        fs::write(&file, "alpha beta gamma").unwrap();

        let fp = file.to_string_lossy().to_string();
        let blocks = vec![
            make_block(&fp, "alpha", "xxx", 1),
            make_block(&fp, "gamma", "zzz", 2),
        ];

        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
        assert_eq!(states[&fp].content_norm, "xxx beta zzz");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_delete_operation() {
        let tmp = create_temp_dir();
        let file = tmp.join("delete.txt");
        fs::write(&file, "remove me").unwrap();

        let blocks = vec![make_block(&file.to_string_lossy(), "remove me", "", 1)];
        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
        assert_eq!(states[&file.to_string_lossy().to_string()].content_norm, "");
        assert!(states[&file.to_string_lossy().to_string()].modified);

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_crlf_preserved() {
        let tmp = create_temp_dir();
        let file = tmp.join("crlf.txt");
        fs::write(&file, "line1\r\nline2\r\nline3\r\n").unwrap();

        let blocks = vec![make_block(&file.to_string_lossy(), "line2", "replaced", 1)];
        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
        let state = &states[&file.to_string_lossy().to_string()];
        assert_eq!(state.line_ending, LineEnding::Crlf);
        assert_eq!(state.content_norm, "line1\nreplaced\nline3\n");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_collects_all_errors() {
        let tmp = create_temp_dir();
        let file = tmp.join("allerrs.txt");
        fs::write(&file, "hello world").unwrap();

        let fp = file.to_string_lossy().to_string();
        let blocks = vec![
            make_block(&fp, "nonexistent1", "x", 1),
            make_block(&fp, "nonexistent2", "y", 2),
        ];

        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 2, "Should collect all errors, not fail fast");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_create_not_first_block() {
        let tmp = create_temp_dir();
        let file = tmp.join("create_order.txt");
        // File does not exist - first block is non-create, second is create
        // Actually, for this test we need a file that doesn't exist, with a
        // non-create block first. But that would trigger FileNotFound.
        // The CreateNotFirstBlock error occurs when a CREATE block appears after
        // a non-create block for the same file. Let's create the file so
        // non-create can work, but then CREATE on existing file triggers
        // CreateOnExistingFile instead. Let's check the logic...
        // Actually, seen_noncreate_for_file is checked before existed_on_disk for create.
        // No, the order in the code is: is_create -> existed_on_disk check first,
        // then seen_noncreate check. So CreateOnExistingFile takes precedence.
        // For CreateNotFirstBlock to trigger, the file must NOT exist on disk
        // and a non-create block must have been seen.
        // But if file doesn't exist and there's no CREATE, it's FileNotFound...
        // The unavailable_files check skips subsequent blocks for that file.
        // Hmm, this path might be hard to trigger. Let me check: what if we have
        // CREATE then another CREATE for same file?
        let file2 = tmp.join("double_create.txt");
        let fp = file2.to_string_lossy().to_string();
        let blocks = vec![
            make_block(&fp, "", "content1", 1),
            make_block(&fp, "", "content2", 2),
        ];

        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].error_type, ValidationErrorType::AmbiguousMatch));

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_multiple_files() {
        let tmp = create_temp_dir();
        let file_a = tmp.join("a.txt");
        let file_b = tmp.join("b.txt");
        fs::write(&file_a, "content_a").unwrap();
        fs::write(&file_b, "content_b").unwrap();

        let blocks = vec![
            make_block(&file_a.to_string_lossy(), "content_a", "new_a", 1),
            make_block(&file_b.to_string_lossy(), "content_b", "new_b", 2),
        ];

        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
        assert_eq!(states[&file_a.to_string_lossy().to_string()].content_norm, "new_a");
        assert_eq!(states[&file_b.to_string_lossy().to_string()].content_norm, "new_b");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_unmodified_file_not_marked_modified() {
        let tmp = create_temp_dir();
        let file_a = tmp.join("mod.txt");
        let file_b = tmp.join("unmod.txt");
        fs::write(&file_a, "change me").unwrap();
        fs::write(&file_b, "leave me").unwrap();

        // Only modify file_a, file_b is referenced but via another file's block
        let blocks = vec![make_block(&file_a.to_string_lossy(), "change me", "changed", 1)];

        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty());
        assert!(states[&file_a.to_string_lossy().to_string()].modified);
        // file_b not in states since not referenced
        assert!(!states.contains_key(&file_b.to_string_lossy().to_string()));

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_validate_overlap_aware_ambiguous() {
        let tmp = create_temp_dir();
        let file = tmp.join("overlap.txt");
        fs::write(&file, "aaaa").unwrap();

        // "aa" in "aaaa" = 3 overlapping matches
        let blocks = vec![make_block(&file.to_string_lossy(), "aa", "xx", 1)];
        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0].error_type, ValidationErrorType::AmbiguousMatch));

        cleanup_temp_dir(&tmp);
    }

    // ==================== Commit (Atomic) ====================

    #[test]
    fn test_commit_update_file() {
        let tmp = create_temp_dir();
        let file = tmp.join("commit_update.txt");
        fs::write(&file, "original content").unwrap();

        let fp = file.to_string_lossy().to_string();
        let mut states = HashMap::new();
        states.insert(fp.clone(), FileState {
            path: fp.clone(),
            line_ending: LineEnding::Lf,
            content_norm: "updated content".to_string(),
            existed_on_disk: true,
            modified: true,
        });

        let result = commit_changes_atomic(&states, 1);
        assert!(result.is_ok(), "Commit failed: {:?}", result.err());
        assert_eq!(fs::read_to_string(&file).unwrap(), "updated content");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_commit_create_file() {
        let tmp = create_temp_dir();
        let file = tmp.join("commit_create.txt");

        let fp = file.to_string_lossy().to_string();
        let mut states = HashMap::new();
        states.insert(fp.clone(), FileState {
            path: fp.clone(),
            line_ending: LineEnding::Lf,
            content_norm: "brand new file".to_string(),
            existed_on_disk: false,
            modified: true,
        });

        let result = commit_changes_atomic(&states, 1);
        assert!(result.is_ok(), "Commit failed: {:?}", result.err());
        assert!(file.exists());
        assert_eq!(fs::read_to_string(&file).unwrap(), "brand new file");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_commit_delete_file() {
        let tmp = create_temp_dir();
        let file = tmp.join("commit_delete.txt");
        fs::write(&file, "to be deleted").unwrap();

        let fp = file.to_string_lossy().to_string();
        let mut states = HashMap::new();
        states.insert(fp.clone(), FileState {
            path: fp.clone(),
            line_ending: LineEnding::Lf,
            content_norm: "".to_string(),
            existed_on_disk: true,
            modified: true,
        });

        let result = commit_changes_atomic(&states, 1);
        assert!(result.is_ok(), "Commit failed: {:?}", result.err());
        assert!(!file.exists());

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_commit_preserves_crlf() {
        let tmp = create_temp_dir();
        let file = tmp.join("commit_crlf.txt");
        fs::write(&file, "old\r\ncontent\r\n").unwrap();

        let fp = file.to_string_lossy().to_string();
        let mut states = HashMap::new();
        states.insert(fp.clone(), FileState {
            path: fp.clone(),
            line_ending: LineEnding::Crlf,
            content_norm: "new\ncontent\n".to_string(),
            existed_on_disk: true,
            modified: true,
        });

        let result = commit_changes_atomic(&states, 1);
        assert!(result.is_ok());
        assert_eq!(fs::read_to_string(&file).unwrap(), "new\r\ncontent\r\n");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_commit_no_modified_files() {
        let states: HashMap<String, FileState> = HashMap::new();
        let result = commit_changes_atomic(&states, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_commit_unmodified_file_skipped() {
        let tmp = create_temp_dir();
        let file = tmp.join("unmod_commit.txt");
        fs::write(&file, "original").unwrap();

        let fp = file.to_string_lossy().to_string();
        let mut states = HashMap::new();
        states.insert(fp.clone(), FileState {
            path: fp.clone(),
            line_ending: LineEnding::Lf,
            content_norm: "original".to_string(),
            existed_on_disk: true,
            modified: false,
        });

        let result = commit_changes_atomic(&states, 1);
        assert!(result.is_ok());
        // File should remain unchanged
        assert_eq!(fs::read_to_string(&file).unwrap(), "original");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_commit_create_with_parent_dirs() {
        let tmp = create_temp_dir();
        let file = tmp.join("sub").join("dir").join("deep.txt");

        let fp = file.to_string_lossy().to_string();
        let mut states = HashMap::new();
        states.insert(fp.clone(), FileState {
            path: fp.clone(),
            line_ending: LineEnding::Lf,
            content_norm: "nested file".to_string(),
            existed_on_disk: false,
            modified: true,
        });

        let result = commit_changes_atomic(&states, 1);
        assert!(result.is_ok(), "Commit failed: {:?}", result.err());
        assert!(file.exists());
        assert_eq!(fs::read_to_string(&file).unwrap(), "nested file");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_commit_delete_removes_empty_parent_dirs() {
        let tmp = create_temp_dir();
        let sub = tmp.join("empty_parent");
        fs::create_dir_all(&sub).unwrap();
        let file = sub.join("doomed.txt");
        fs::write(&file, "content").unwrap();

        let fp = file.to_string_lossy().to_string();
        let mut states = HashMap::new();
        states.insert(fp.clone(), FileState {
            path: fp.clone(),
            line_ending: LineEnding::Lf,
            content_norm: "".to_string(),
            existed_on_disk: true,
            modified: true,
        });

        let result = commit_changes_atomic(&states, 1);
        assert!(result.is_ok());
        assert!(!file.exists());
        // empty_parent dir should be removed (best-effort)
        assert!(!sub.exists());

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_commit_multiple_files() {
        let tmp = create_temp_dir();
        let file_a = tmp.join("multi_a.txt");
        let file_b = tmp.join("multi_b.txt");
        fs::write(&file_a, "old_a").unwrap();
        fs::write(&file_b, "old_b").unwrap();

        let mut states = HashMap::new();
        states.insert(file_a.to_string_lossy().to_string(), FileState {
            path: file_a.to_string_lossy().to_string(),
            line_ending: LineEnding::Lf,
            content_norm: "new_a".to_string(),
            existed_on_disk: true,
            modified: true,
        });
        states.insert(file_b.to_string_lossy().to_string(), FileState {
            path: file_b.to_string_lossy().to_string(),
            line_ending: LineEnding::Lf,
            content_norm: "new_b".to_string(),
            existed_on_disk: true,
            modified: true,
        });

        let result = commit_changes_atomic(&states, 2);
        assert!(result.is_ok());
        assert_eq!(fs::read_to_string(&file_a).unwrap(), "new_a");
        assert_eq!(fs::read_to_string(&file_b).unwrap(), "new_b");

        cleanup_temp_dir(&tmp);
    }

    // ==================== Temp Path Helpers ====================

    #[test]
    fn test_unique_sibling_path() {
        let tmp = create_temp_dir();
        let target = tmp.join("file.txt");

        let p1 = unique_sibling_path(&target, ".fsrb.tmp").unwrap();
        assert!(p1.to_string_lossy().contains(".fsrb.tmp"));
        assert!(!p1.exists());

        // Creating the file and calling again should give a different path
        fs::write(&p1, "x").unwrap();
        let p2 = unique_sibling_path(&target, ".fsrb.tmp").unwrap();
        assert_ne!(p1, p2);

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_create_new_file_helper() {
        let tmp = create_temp_dir();
        let file = tmp.join("created.txt");

        let mut f = create_new_file(&file).unwrap();
        f.write_all(b"hello").unwrap();
        drop(f);

        assert_eq!(fs::read_to_string(&file).unwrap(), "hello");

        // Trying to create again should fail (create_new semantics)
        assert!(create_new_file(&file).is_err());

        cleanup_temp_dir(&tmp);
    }

    // ==================== ensure_parent_dirs_for_create ====================

    #[test]
    fn test_ensure_parent_dirs_creates_nested() {
        let tmp = create_temp_dir();
        let nested = tmp.join("newdir1").join("newdir2").join("newdir3");
        let target = nested.join("file.txt");
        assert!(!nested.exists());

        let created = ensure_parent_dirs_for_create(&target).unwrap();
        assert!(nested.exists());
        // created list contains the directories we made (bottom-up order)
        assert!(!created.is_empty());
        // All created dirs should exist
        for d in &created {
            assert!(d.exists());
        }

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_ensure_parent_dirs_existing_parent_noop() {
        let tmp = create_temp_dir();
        let target = tmp.join("existing_file.txt");
        // Parent already exists (tmp)
        let created = ensure_parent_dirs_for_create(&target).unwrap();
        assert!(created.is_empty());

        cleanup_temp_dir(&tmp);
    }

    // ==================== remove_empty_dirs_best_effort ====================

    #[test]
    fn test_remove_empty_dirs_best_effort() {
        let tmp = create_temp_dir();
        let d1 = tmp.join("removeme1");
        let d2 = tmp.join("removeme2");
        fs::create_dir(&d1).unwrap();
        fs::create_dir(&d2).unwrap();

        remove_empty_dirs_best_effort(&[d1.clone(), d2.clone()]);
        assert!(!d1.exists());
        assert!(!d2.exists());

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_remove_empty_dirs_nonempty_skipped() {
        let tmp = create_temp_dir();
        let d = tmp.join("notempty");
        fs::create_dir(&d).unwrap();
        fs::write(d.join("file.txt"), "x").unwrap();

        remove_empty_dirs_best_effort(&[d.clone()]);
        assert!(d.exists()); // Should still exist because not empty

        cleanup_temp_dir(&tmp);
    }

    // ==================== remove_empty_parent_dirs_best_effort ====================

    #[test]
    fn test_remove_empty_parent_dirs() {
        let tmp = create_temp_dir();
        let deep = tmp.join("p1").join("p2").join("p3");
        fs::create_dir_all(&deep).unwrap();
        let file = deep.join("file.txt");
        // Don't actually create the file, just test parent cleanup
        // The file doesn't exist but its parent dirs do
        remove_empty_parent_dirs_best_effort(&file);
        assert!(!deep.exists());
        assert!(!tmp.join("p1").join("p2").exists());
        assert!(!tmp.join("p1").exists());

        cleanup_temp_dir(&tmp);
    }

    // ==================== Error Reporting ====================

    #[test]
    fn test_print_error_report_runs_without_panic() {
        let errors = vec![
            ValidationError {
                file_path: "file_a.txt".to_string(),
                block_index: 1,
                error_type: ValidationErrorType::SearchBlockNotFound,
                message: "SEARCH block not found.".to_string(),
                snippet: "some snippet".to_string(),
            },
            ValidationError {
                file_path: "file_a.txt".to_string(),
                block_index: 2,
                error_type: ValidationErrorType::AmbiguousMatch,
                message: "Found 3 occurrences.".to_string(),
                snippet: "another snippet".to_string(),
            },
            ValidationError {
                file_path: "file_b.txt".to_string(),
                block_index: 1,
                error_type: ValidationErrorType::FileNotFound,
                message: "File not found on disk.".to_string(),
                snippet: String::new(),
            },
        ];
        // Just ensure it doesn't panic with all error types
        print_error_report(&errors);
    }

    #[test]
    fn test_print_error_report_all_error_types() {
        let errors = vec![
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 1,
                error_type: ValidationErrorType::FileNotFound,
                message: "m".to_string(), snippet: String::new(),
            },
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 2,
                error_type: ValidationErrorType::FileUnreadable,
                message: "m".to_string(), snippet: String::new(),
            },
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 3,
                error_type: ValidationErrorType::EmptySearchBlock,
                message: "m".to_string(), snippet: String::new(),
            },
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 4,
                error_type: ValidationErrorType::SearchBlockNotFound,
                message: "m".to_string(), snippet: "s".to_string(),
            },
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 5,
                error_type: ValidationErrorType::AmbiguousMatch,
                message: "m".to_string(), snippet: "s".to_string(),
            },
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 6,
                error_type: ValidationErrorType::InvalidEmptySearchAndReplace,
                message: "m".to_string(), snippet: String::new(),
            },
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 7,
                error_type: ValidationErrorType::CreateOnExistingFile,
                message: "m".to_string(), snippet: String::new(),
            },
            ValidationError {
                file_path: "f.txt".to_string(), block_index: 8,
                error_type: ValidationErrorType::CreateNotFirstBlock,
                message: "m".to_string(), snippet: String::new(),
            },
        ];
        // Should not panic — all variants covered
        print_error_report(&errors);
    }

    // ==================== End-to-End: Parse + Validate + Commit ====================

    #[test]
    fn test_end_to_end_update() {
        let tmp = create_temp_dir();
        let file = tmp.join("e2e.txt");
        fs::write(&file, "hello world\ngoodbye moon\n").unwrap();

        let patch = format!(
            "{}\n<<<<<<< SEARCH\nhello world\n=======\nhello rust\n>>>>>>> REPLACE\n",
            file.display()
        );

        let blocks = parse_patch_file(&patch).unwrap();
        assert_eq!(blocks.len(), 1);

        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty());

        let result = commit_changes_atomic(&states, blocks.len());
        assert!(result.is_ok());
        assert_eq!(fs::read_to_string(&file).unwrap(), "hello rust\ngoodbye moon\n");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_end_to_end_create() {
        let tmp = create_temp_dir();
        let file = tmp.join("e2e_new.txt");

        let patch = format!(
            "{}\n<<<<<<< SEARCH\n=======\nnew file content\n>>>>>>> REPLACE\n",
            file.display()
        );

        let blocks = parse_patch_file(&patch).unwrap();
        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty());

        let result = commit_changes_atomic(&states, blocks.len());
        assert!(result.is_ok());
        assert_eq!(fs::read_to_string(&file).unwrap(), "new file content");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_end_to_end_delete() {
        let tmp = create_temp_dir();
        let file = tmp.join("e2e_del.txt");
        fs::write(&file, "doomed content").unwrap();

        let patch = format!(
            "{}\n<<<<<<< SEARCH\ndoomed content\n=======\n>>>>>>> REPLACE\n",
            file.display()
        );

        let blocks = parse_patch_file(&patch).unwrap();
        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty());

        let result = commit_changes_atomic(&states, blocks.len());
        assert!(result.is_ok());
        assert!(!file.exists());

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_end_to_end_validation_failure_no_changes() {
        let tmp = create_temp_dir();
        let file = tmp.join("e2e_nochg.txt");
        fs::write(&file, "original content").unwrap();

        let patch = format!(
            "{}\n<<<<<<< SEARCH\nwrong content\n=======\nreplacement\n>>>>>>> REPLACE\n",
            file.display()
        );

        let blocks = parse_patch_file(&patch).unwrap();
        let (_states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(!errors.is_empty());

        // File should be untouched since we didn't commit
        assert_eq!(fs::read_to_string(&file).unwrap(), "original content");

        cleanup_temp_dir(&tmp);
    }

    #[test]
    fn test_end_to_end_multi_block_multi_file() {
        let tmp = create_temp_dir();
        let file_a = tmp.join("e2e_a.txt");
        let file_b = tmp.join("e2e_b.txt");
        fs::write(&file_a, "alpha content").unwrap();
        fs::write(&file_b, "beta content").unwrap();

        let patch = format!(
            "{fa}\n<<<<<<< SEARCH\nalpha content\n=======\nalpha updated\n>>>>>>> REPLACE\n\n\
             {fb}\n<<<<<<< SEARCH\nbeta content\n=======\nbeta updated\n>>>>>>> REPLACE\n",
            fa = file_a.display(),
            fb = file_b.display()
        );

        let blocks = parse_patch_file(&patch).unwrap();
        assert_eq!(blocks.len(), 2);

        let (states, errors) = validate_and_apply_in_memory(&blocks);
        assert!(errors.is_empty());

        let result = commit_changes_atomic(&states, blocks.len());
        assert!(result.is_ok());
        assert_eq!(fs::read_to_string(&file_a).unwrap(), "alpha updated");
        assert_eq!(fs::read_to_string(&file_b).unwrap(), "beta updated");

        cleanup_temp_dir(&tmp);
    }
}
