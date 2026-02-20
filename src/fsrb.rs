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
    let mut created_dirs: Vec<PathBuf> = Vec::new();
    if let Some(parent) = target_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            // Create parent directories as needed.
            fs::create_dir_all(parent)?;
            // Track which directories we actually created (for rollback on error).
            let mut check_path = parent.to_path_buf();
            while check_path.components().count() > 0 { // avoid infinite loop on root
                if check_path.exists() && !created_dirs.contains(&check_path) {
                    created_dirs.push(check_path.clone());
                } else {
                    // Optimization: if check_path exists and we didn't just create it, assume parents exist.
                    // However, for strict rollback, we might just track what we create.
                    // fs::create_dir_all is idempotent.
                    // A simpler way: we can't easily know exactly which ones WE created vs existed.
                    // But we can just try to remove them if empty on rollback.
                    // Let's iterate bottom-up and add to list.
                }
                if let Some(p) = check_path.parent() {
                    check_path = p.to_path_buf();
                } else {
                    break;
                }
            }
        }
    }
    // Since precise tracking is hard with create_dir_all, we'll simplify:
    // just try to remove empty parents starting from the target's parent upwards.
    // But create_dir_all returns () on success.
    // For now, let's just return an empty vec and rely on best-effort cleanup?
    // The Python version does: _ensure_parent_dirs_for_create returns list.
    // Let's do it properly: iterate from top-most missing parent down to target parent.

    // Reset created_dirs
    let mut actual_created: Vec<PathBuf> = Vec::new();
    if let Some(parent) = target_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
             // Identify the first existing parent
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
             // Now create them from top to bottom
             while let Some(dir) = stack.pop() {
                 fs::create_dir(&dir)?;
                 actual_created.push(dir);
             }
        }
    }
    // Return in reverse order (bottom-up) for easier removal
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
                ValidationErrorType::FileNotFound => error.message.clone(),
                ValidationErrorType::FileUnreadable => error.message.clone(),
                ValidationErrorType::EmptySearchBlock => error.message.clone(),
                ValidationErrorType::SearchBlockNotFound => error.message.clone(),
                ValidationErrorType::AmbiguousMatch => error.message.clone(),
            };

            println!("  - Block #{}: {}", error.block_index, msg);
            if !error.snippet.is_empty() {
                println!("    Snippet: \"{}\"", error.snippet);
            }
        }
        println!();
    }
}
