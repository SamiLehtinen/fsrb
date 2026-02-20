<#
.SYNOPSIS
    fsrb — Applies deterministic delimiter-based “Search & Replace” patches to text files
    with strict all-or-nothing transactionality.

.DESCRIPTION
    Patch format (Aider-compatible):

        path/to/target.ext
        <<<<<<< SEARCH
        [Exact original content to find]
        =======
        [New content to replace with]
        >>>>>>> REPLACE

    Escaping delimiter lines inside SEARCH/REPLACE content:
      - If you need a literal line equal to a delimiter marker, prefix the line with a single
        backslash (`\`) at the very start of the line (e.g., `\<<<<<<< SEARCH`).
      - The leading `\` is removed during parsing and the literal delimiter text is included.

    Rules:
      - SEARCH content is matched literally (no regex).
      - Each SEARCH block must match exactly once (overlap-aware counting).
      - Empty SEARCH block is an error.
      - Validation is done in memory across all files/blocks; if any error occurs,
        no files are modified.
      - On success, files are written via an atomic temp+rename strategy with rollback.
      - Matching is done on normalized LF in memory; original line endings are preserved.

.PARAMETER PatchFile
    Path to the patch file.

.EXAMPLE
    .\file_search_replace.ps1 -PatchFile .\update.fsrb
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $PatchFile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:FsrbBaseDir = (Get-Location).ProviderPath

# ---------------------------
# Helpers: Encoding / Text
# ---------------------------

function New-StrictUtf8Encoding {
    # UTF-8 without BOM, but throws on invalid bytes (closest to Rust read_to_string expectations).
    return [System.Text.UTF8Encoding]::new($false, $true)
}

function Read-AllTextStrictUtf8([string] $Path) {
    # Use StreamReader with detectEncodingFromByteOrderMarks=true to handle BOMs,
    # while still enforcing Strict UTF-8 (throw on invalid bytes) via the encoding object.
    $enc = New-StrictUtf8Encoding
    $reader = [System.IO.StreamReader]::new($Path, $enc, $true)
    try {
        return $reader.ReadToEnd()
    }
    finally {
        $reader.Dispose()
    }
}

function Write-AllTextStrictUtf8([string] $Path, [string] $Content) {
    $enc = New-StrictUtf8Encoding
    # Write via FileStream for CreateNew and flush-to-disk support where possible.
    $bytes = $enc.GetBytes($Content)
    $fs = $null
    try {
        $fs = [System.IO.File]::Open($Path,
            [System.IO.FileMode]::CreateNew,
            [System.IO.FileAccess]::Write,
            [System.IO.FileShare]::None
        )
        $fs.Write($bytes, 0, $bytes.Length)
        $fs.Flush($true)
    }
    finally {
        if ($fs) { $fs.Dispose() }
    }
}

function Detect-LineEnding([string] $Content) {
    if ($Content.Contains("`r`n")) { return 'Crlf' }
    return 'Lf'
}

function Normalize-ToLf([string] $Text) {
    # Convert CRLF -> LF; leave lone CR untouched.
    return $Text.Replace("`r`n", "`n")
}

function Convert-LfToCrlf([string] $Text) {
    # Assumes input uses only LF.
    return $Text.Replace("`n", "`r`n")
}

function Get-Snippet([string] $Text) {
    $first = ($Text -split "`n", 2, [System.StringSplitOptions]::None)[0]
    if ($first.Length -gt 60) { return $first.Substring(0, 60) + "..." }
    return $first
}

# ---------------------------
# Helpers: Matching / Replace
# ---------------------------

function Find-UniqueOverlapping {
    param(
        [Parameter(Mandatory = $true)][string] $Haystack,
        [Parameter(Mandatory = $true)][string] $Needle
    )

    if ([string]::IsNullOrEmpty($Needle)) {
        return @{ Kind = 'None' }
    }
    if ($Needle.Length -gt $Haystack.Length) {
        return @{ Kind = 'None' }
    }

    $count = 0
    $firstPos = -1
    $start = 0

    while ($true) {
        $pos = $Haystack.IndexOf($Needle, $start, [System.StringComparison]::Ordinal)
        if ($pos -lt 0) { break }

        $count++
        if ($count -eq 1) { $firstPos = $pos }

        # allow overlaps by advancing just one char
        $start = $pos + 1
    }

    switch ($count) {
        0 { return @{ Kind = 'None' } }
        1 { return @{ Kind = 'One'; Start = $firstPos } }
        default { return @{ Kind = 'Ambiguous'; Count = $count } }
    }
}

function Replace-At {
    param(
        [Parameter(Mandatory = $true)][string] $Haystack,
        [Parameter(Mandatory = $true)][int] $Start,
        [Parameter(Mandatory = $true)][int] $NeedleLen,
        [string] $Replacement = ""
    )

    $prefix = if ($Start -gt 0) { $Haystack.Substring(0, $Start) } else { "" }
    $suffixStart = $Start + $NeedleLen
    $suffix = if ($suffixStart -lt $Haystack.Length) { $Haystack.Substring($suffixStart) } else { "" }
    return $prefix + $Replacement + $suffix
}

# ---------------------------
# Helpers: Temp/Backup Paths
# ---------------------------

function New-UniqueSiblingPath {
    param(
        [Parameter(Mandatory = $true)][System.IO.FileInfo] $Target,
        [Parameter(Mandatory = $true)][string] $Tag
    )

    $parent = $Target.Directory
    if (-not $parent) {
        throw "Target path has no parent directory: $($Target.FullName)"
    }

    $fileName = $Target.Name
    $procId = $PID
    # use ticks (100ns) to approximate "nanos"
    $ticks = [System.DateTimeOffset]::UtcNow.UtcTicks

    for ($n = 0; $n -lt 10000; $n++) {
        $candidateName = "{0}.{1}.{2}.{3}.{4}" -f $fileName, $Tag, $procId, $ticks, $n
        $candidatePath = [System.IO.Path]::Combine($parent.FullName, $candidateName)
        if (-not [System.IO.File]::Exists($candidatePath)) {
            return $candidatePath
        }
    }

    throw "Unable to allocate unique temp path for $($Target.FullName)"
}

function Try-CopyAttributesBestEffort {
    param(
        [Parameter(Mandatory = $true)][string] $FromPath,
        [Parameter(Mandatory = $true)][string] $ToPath
    )
    try {
        $attrs = [System.IO.File]::GetAttributes($FromPath)
        [System.IO.File]::SetAttributes($ToPath, $attrs)
    }
    catch {
        # best-effort, ignore
    }
}

# ---------------------------
# Parsing
# ---------------------------

function Parse-PatchFile {
    param(
        [Parameter(Mandatory = $true)][string] $Content
    )

    $blocks = New-Object System.Collections.Generic.List[object]

    # Similar to Python: splitlines() and Rust: content.lines()
    $lines = [System.Collections.Generic.List[string]]::new()
    foreach ($l in ([System.Text.RegularExpressions.Regex]::Split($Content, "\r\n|\n|\r"))) {
        $lines.Add($l)
    }

    $currentFile = $null
    $blockCounter = 0

    function Unescape-DelimiterLine([string] $Line) {
        # Escape is only recognized when "\" is the very first character of the line.
        if ($Line.StartsWith('\')) {
            $candidate = $Line.Substring(1)
            if ($candidate -eq '<<<<<<< SEARCH' -or $candidate -eq '=======' -or $candidate -eq '>>>>>>> REPLACE') {
                return $candidate
            }
        }
        return $Line
    }

    function Resolve-TargetPath([string] $HeaderPath) {
        if ([string]::IsNullOrWhiteSpace($HeaderPath)) { return $HeaderPath }

        $p = $HeaderPath
        if (-not [System.IO.Path]::IsPathRooted($p)) {
            $p = [System.IO.Path]::Combine($script:FsrbBaseDir, $p)
        }
        return [System.IO.Path]::GetFullPath($p)
    }

    $i = 0
    while ($i -lt $lines.Count) {
        $line = $lines[$i]
        $trimmed = $line.Trim()

        # Ignore markdown code fences
        if ($trimmed.StartsWith('```')) {
            $i++
            continue
        }

        # Aider-compatible header: a plain path line, only if next non-ignored line is SEARCH
        if ($trimmed.Length -gt 0 `
            -and $trimmed -ne '<<<<<<< SEARCH' `
            -and $trimmed -ne '=======' `
            -and $trimmed -ne '>>>>>>> REPLACE' `
            -and -not $trimmed.StartsWith('#')) {

            $j = $i + 1
            while ($j -lt $lines.Count) {
                $t = $lines[$j].Trim()
                if ($t.Length -eq 0 -or $t.StartsWith('#') -or $t.StartsWith('```')) {
                    $j++
                    continue
                }
                break
            }

            if ($j -lt $lines.Count -and $lines[$j].Trim() -eq '<<<<<<< SEARCH') {
                $currentFile = Resolve-TargetPath $trimmed
                $i++
                continue
            }
        }

        # SEARCH block start
        if ($trimmed -eq '<<<<<<< SEARCH') {
            if ([string]::IsNullOrEmpty($currentFile)) {
                throw ("Line {0}: Found SEARCH block without a preceding file path header line." -f ($i + 1))
            }

            $blockCounter++
            $startLine = $i + 1
            $i++

            $searchLines = New-Object System.Collections.Generic.List[string]
            while ($i -lt $lines.Count -and $lines[$i].Trim() -ne '=======') {
                $searchLines.Add((Unescape-DelimiterLine $lines[$i]))
                $i++
            }
            if ($i -ge $lines.Count) {
                throw ("Block #{0}: Unexpected end of file while reading SEARCH block." -f $blockCounter)
            }

            # skip '======='
            $i++

            $replaceLines = New-Object System.Collections.Generic.List[string]
            while ($i -lt $lines.Count -and $lines[$i].Trim() -ne '>>>>>>> REPLACE') {
                $replaceLines.Add((Unescape-DelimiterLine $lines[$i]))
                $i++
            }
            if ($i -ge $lines.Count) {
                throw ("Block #{0}: Unexpected end of file while reading REPLACE block." -f $blockCounter)
            }

            # skip '>>>>>>> REPLACE'
            $i++

            $search = Normalize-ToLf(($searchLines -join "`n"))
            $replace = Normalize-ToLf(($replaceLines -join "`n"))

            $blocks.Add([pscustomobject]@{
                FilePath       = $currentFile
                SearchPattern  = $search
                ReplacePattern = $replace
                BlockIndex     = $blockCounter
                LineNumber     = $startLine
            }) | Out-Null

            continue
        }

        # ignore everything else
        $i++
    }

    return ,$blocks
}

# ---------------------------
# Validation & In-Memory Apply
# ---------------------------

function Validate-AndApplyInMemory {
    param(
        [Parameter(Mandatory = $true)][object[]] $Blocks
    )

    # file_states: path -> state
    $fileStates = @{}
    $errors = New-Object System.Collections.Generic.List[object]

    # Unique paths in deterministic order (Case-insensitive for Windows safety)
    $unique = New-Object System.Collections.Generic.SortedSet[string] ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($b in $Blocks) { [void]$unique.Add([string]$b.FilePath) }

    # Map file -> first block index
    $firstBlockForFile = @{}
    foreach ($b in $Blocks) {
        $p = [string]$b.FilePath
        $idx = [int]$b.BlockIndex
        if (-not $firstBlockForFile.ContainsKey($p) -or $idx -lt $firstBlockForFile[$p]) {
            $firstBlockForFile[$p] = $idx
        }
    }

    # Pre-scan which files are intended to be created (empty SEARCH + non-empty REPLACE)
    $createFiles = New-Object System.Collections.Generic.SortedSet[string] ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($b in $Blocks) {
        $search = [string]$b.SearchPattern
        $replace = [string]$b.ReplacePattern
        if ($search -eq "" -and $replace -ne "") {
            [void]$createFiles.Add([string]$b.FilePath)
        }
    }

    $unavailable = New-Object System.Collections.Generic.SortedSet[string] ([System.StringComparer]::OrdinalIgnoreCase)

    # Load all referenced files (or create empty in-memory states for CREATE targets)
    foreach ($path in $unique) {
        if ([System.IO.File]::Exists($path)) {
            try {
                $content = Read-AllTextStrictUtf8 $path
                $lineEnding = Detect-LineEnding $content
                $contentNorm = Normalize-ToLf $content

                $fileStates[$path] = [pscustomobject]@{
                    Path           = $path
                    LineEnding     = $lineEnding  # 'Lf' or 'Crlf'
                    ContentNorm    = $contentNorm
                    ExistedOnDisk  = $true
                    Modified       = $false
                }
            }
            catch {
                $errors.Add([pscustomobject]@{
                    FilePath   = $path
                    BlockIndex = ($firstBlockForFile[$path] | ForEach-Object { $_ })
                    ErrorType  = 'FileUnreadable'
                    Message    = ("Error reading file: {0}" -f $_.Exception.Message)
                    Snippet    = ""
                }) | Out-Null
                [void]$unavailable.Add($path)
            }
        }
        else {
            if ($createFiles.Contains($path)) {
                # CREATE targets start from an empty, non-existent state
                $fileStates[$path] = [pscustomobject]@{
                    Path           = $path
                    LineEnding     = 'Lf'
                    ContentNorm    = ""
                    ExistedOnDisk  = $false
                    Modified       = $false
                }
            }
            else {
                $errors.Add([pscustomobject]@{
                    FilePath   = $path
                    BlockIndex = ($firstBlockForFile[$path] | ForEach-Object { $_ })
                    ErrorType  = 'FileNotFound'
                    Message    = "File not found on disk."
                    Snippet    = ""
                }) | Out-Null
                [void]$unavailable.Add($path)
            }
        }
    }

    # Track whether a CREATE has already been applied per file, and enforce ordering
    $seenNonCreateForFile = @{}
    $seenCreateForFile = @{}

    foreach ($block in $Blocks) {
        $filePath = [string]$block.FilePath
        if ($unavailable.Contains($filePath)) { continue }

        $state = $fileStates[$filePath]
        if (-not $state) {
            $errors.Add([pscustomobject]@{
                FilePath   = $filePath
                BlockIndex = [int]$block.BlockIndex
                ErrorType  = 'FileUnreadable'
                Message    = "Internal error: file state not loaded."
                Snippet    = ""
            }) | Out-Null
            continue
        }

        $search = [string]$block.SearchPattern
        $replace = [string]$block.ReplacePattern

        $isCreate = ($search -eq "" -and $replace -ne "")
        $isInvalidEmpty = ($search -eq "" -and $replace -eq "")

        if ($isInvalidEmpty) {
            $errors.Add([pscustomobject]@{
                FilePath   = $filePath
                BlockIndex = [int]$block.BlockIndex
                ErrorType  = 'InvalidEmptySearchAndReplace'
                Message    = "Invalid block: SEARCH and REPLACE are both empty."
                Snippet    = ""
            }) | Out-Null
            continue
        }

        if ($isCreate) {
            if ($state.ExistedOnDisk) {
                $errors.Add([pscustomobject]@{
                    FilePath   = $filePath
                    BlockIndex = [int]$block.BlockIndex
                    ErrorType  = 'CreateOnExistingFile'
                    Message    = "CREATE block is only valid when the target file does not exist."
                    Snippet    = ""
                }) | Out-Null
                continue
            }

            if ($seenNonCreateForFile.ContainsKey($filePath) -and $seenNonCreateForFile[$filePath]) {
                $errors.Add([pscustomobject]@{
                    FilePath   = $filePath
                    BlockIndex = [int]$block.BlockIndex
                    ErrorType  = 'CreateNotFirstBlock'
                    Message    = "CREATE block must be the first block for its file."
                    Snippet    = ""
                }) | Out-Null
                continue
            }

            if ($seenCreateForFile.ContainsKey($filePath) -and $seenCreateForFile[$filePath]) {
                $errors.Add([pscustomobject]@{
                    FilePath   = $filePath
                    BlockIndex = [int]$block.BlockIndex
                    ErrorType  = 'AmbiguousMatch'
                    Message    = "Multiple CREATE blocks for the same file are not allowed."
                    Snippet    = ""
                }) | Out-Null
                continue
            }

            # Apply CREATE by setting the entire file content from empty
            $state.ContentNorm = $replace
            $state.Modified = $true
            $seenCreateForFile[$filePath] = $true
            continue
        }

        # Non-CREATE blocks: enforce legacy rule that SEARCH must not be empty
        if ($search -eq "") {
            $errors.Add([pscustomobject]@{
                FilePath   = $filePath
                BlockIndex = [int]$block.BlockIndex
                ErrorType  = 'EmptySearchBlock'
                Message    = "SEARCH block is empty."
                Snippet    = ""
            }) | Out-Null
            continue
        }

        $seenNonCreateForFile[$filePath] = $true

        $match = Find-UniqueOverlapping -Haystack $state.ContentNorm -Needle $search

        switch ($match.Kind) {
            'None' {
                $errors.Add([pscustomobject]@{
                    FilePath   = $filePath
                    BlockIndex = [int]$block.BlockIndex
                    ErrorType  = 'SearchBlockNotFound'
                    Message    = "SEARCH block not found in file content."
                    Snippet    = (Get-Snippet $search)
                }) | Out-Null
            }
            'Ambiguous' {
                $errors.Add([pscustomobject]@{
                    FilePath   = $filePath
                    BlockIndex = [int]$block.BlockIndex
                    ErrorType  = 'AmbiguousMatch'
                    Message    = ("Ambiguous match. Found {0} occurrences." -f $match.Count)
                    Snippet    = (Get-Snippet $search)
                }) | Out-Null
            }
            'One' {
                $state.ContentNorm = Replace-At -Haystack $state.ContentNorm -Start ([int]$match.Start) -NeedleLen $search.Length -Replacement $replace
                $state.Modified = $true
            }
            default {
                throw "Internal error: unknown match kind '$($match.Kind)'."
            }
        }
    }

    return @{
        FileStates = $fileStates
        Errors     = ,$errors
    }
}

# ---------------------------
# Error Reporting
# ---------------------------

function Print-ErrorReport {
    param(
        [Parameter(Mandatory = $true)][object[]] $Errors
    )

    Write-Output "[ERROR] Validation failed."
    Write-Output ""

    # Group by file
    $byFile = @{}
    foreach ($e in $Errors) {
        $p = [string]$e.FilePath
        if (-not $byFile.ContainsKey($p)) { $byFile[$p] = New-Object System.Collections.Generic.List[object] }
        $byFile[$p].Add($e) | Out-Null
    }

    $files = $byFile.Keys | Sort-Object
    foreach ($f in $files) {
        Write-Output ("File: {0}" -f $f)
        $fileErrors = $byFile[$f] | Sort-Object -Property BlockIndex

        foreach ($e in $fileErrors) {
            Write-Output ("  - Block #{0}: {1}" -f [int]$e.BlockIndex, [string]$e.Message)
            if (-not [string]::IsNullOrEmpty([string]$e.Snippet)) {
                Write-Output ("    Snippet: ""{0}""" -f [string]$e.Snippet)
            }
        }
        Write-Output ""
    }
}

# ---------------------------
# Commit (Atomic, Rollback-Safe)
# ---------------------------

function Ensure-ParentDirsForCreate {
    param(
        [Parameter(Mandatory = $true)][string] $TargetPath
    )

    $parent = [System.IO.Path]::GetDirectoryName($TargetPath)
    if ([string]::IsNullOrEmpty($parent)) {
        return @()
    }

    $missing = New-Object System.Collections.Generic.List[string]
    $p = $parent
    while ($p -and -not [System.IO.Directory]::Exists($p)) {
        $missing.Add($p)
        $newP = [System.IO.Path]::GetDirectoryName($p)
        if ($newP -eq $p) { break }
        $p = $newP
    }

    $created = New-Object System.Collections.Generic.List[string]
    for ($i = $missing.Count - 1; $i -ge 0; $i--) {
        try {
            [System.IO.Directory]::CreateDirectory($missing[$i]) | Out-Null
            $created.Add($missing[$i])
        }
        catch [System.IO.IOException] {
            # Directory might have been created by another process
        }
    }

    return ,$created.ToArray()
}

function Remove-EmptyDirsBestEffort {
    param(
        [Parameter(Mandatory = $true)][string[]] $Dirs
    )

    # Remove in reverse (deepest first)
    for ($i = $Dirs.Count - 1; $i -ge 0; $i--) {
        try {
            [System.IO.Directory]::Delete($Dirs[$i])
        }
        catch {
            # best-effort, ignore
        }
    }
}

function Remove-EmptyParentDirsBestEffort {
    param(
        [Parameter(Mandatory = $true)][string] $FilePath
    )

    $parent = [System.IO.Path]::GetDirectoryName($FilePath)
    while ($parent -and $parent -ne "." -and $parent -ne [System.IO.Path]::DirectorySeparatorChar.ToString()) {
        try {
            [System.IO.Directory]::Delete($parent)
        }
        catch {
            break
        }
        $parent = [System.IO.Path]::GetDirectoryName($parent)
    }
}

function Rollback-Swaps {
    param(
        [Parameter(Mandatory = $true)][object[]] $Units,
        [Parameter(Mandatory = $true)][string[]] $InstalledNews,
        [Parameter(Mandatory = $true)][string[]] $MovedOriginals
    )

    $errs = New-Object System.Collections.Generic.List[string]

    # 1) For targets with new installed: restore backup over it (reverse order)
    for ($k = $InstalledNews.Count - 1; $k -ge 0; $k--) {
        $target = $InstalledNews[$k]
        $unit = $Units | Where-Object { $_.TargetPath -eq $target } | Select-Object -First 1
        if (-not $unit) { continue }

        if ($unit.Action -eq "create") {
            # Roll back to non-existence
            try {
                if ([System.IO.File]::Exists($target)) {
                    [System.IO.File]::Delete($target)
                }
            }
            catch {
                $errs.Add(("Rollback: failed to remove created file {0}: {1}" -f $target, $_.Exception.Message)) | Out-Null
            }
            continue
        }

        # update: restore backup over it
        $backup = $unit.BackupPath
        if ([string]::IsNullOrEmpty($backup)) { continue }

        try {
            $targetFi = [System.IO.FileInfo]::new($target)
            $rollTmp = New-UniqueSiblingPath -Target $targetFi -Tag ".fsrb.rollback"

            [System.IO.File]::Move($target, $rollTmp)
            [System.IO.File]::Move($backup, $target)
            try { [System.IO.File]::Delete($rollTmp) } catch { }
        }
        catch {
            $errs.Add(("Rollback: failed for {0}: {1}" -f $target, $_.Exception.Message)) | Out-Null
        }
    }

    # 2) For originals moved but maybe no new installed (delete): restore backup if target missing
    for ($k = $MovedOriginals.Count - 1; $k -ge 0; $k--) {
        $target = $MovedOriginals[$k]
        $unit = $Units | Where-Object { $_.TargetPath -eq $target } | Select-Object -First 1
        if (-not $unit) { continue }
        $backup = $unit.BackupPath
        if ([string]::IsNullOrEmpty($backup)) { continue }

        try {
            if ([System.IO.File]::Exists($target)) { continue }
            if ([System.IO.File]::Exists($backup)) {
                [System.IO.File]::Move($backup, $target)
            }
        }
        catch {
            $errs.Add(("Rollback: failed to restore moved original ({0} -> {1}): {2}" -f $backup, $target, $_.Exception.Message)) | Out-Null
        }
    }

    if ($errs.Count -eq 0) { return $null }
    return ($errs -join " | ")
}

function Cleanup-Leftovers {
    param(
        [Parameter(Mandatory = $true)][object[]] $Units
    )

    foreach ($u in $Units) {
        if ($u.TempPath) {
            try { [System.IO.File]::Delete([string]$u.TempPath) } catch { }
        }
        if ($u.BackupPath) {
            try { [System.IO.File]::Delete([string]$u.BackupPath) } catch { }
        }
    }
}

function Commit-ChangesAtomic {
    param(
        [Parameter(Mandatory = $true)][hashtable] $FileStates,
        [Parameter(Mandatory = $true)][int] $TotalBlocks
    )

    $paths = $FileStates.Keys | Sort-Object

    $units = New-Object System.Collections.Generic.List[object]
    foreach ($p in $paths) {
        $st = $FileStates[$p]
        if (-not $st.Modified) { continue }

        # Logic:
        # Create: if not existed_on_disk and content != ""
        # Delete: if existed_on_disk and content is empty/whitespace
        # Update: if existed_on_disk and content is not empty
        $isEmptyContent = ([string]$st.ContentNorm).Trim() -eq ""

        if ((-not $st.ExistedOnDisk) -and -not $isEmptyContent) {
            $action = "create"
        }
        elseif ($st.ExistedOnDisk -and $isEmptyContent) {
            $action = "delete"
        }
        elseif ($st.ExistedOnDisk -and -not $isEmptyContent) {
            $action = "update"
        }
        else {
            # e.g., created then deleted within the same transaction => no-op on disk
            continue
        }

        $final = if ($st.LineEnding -eq 'Crlf') { Convert-LfToCrlf $st.ContentNorm } else { [string]$st.ContentNorm }

        $units.Add([pscustomobject]@{
            TargetPath   = [string]$st.Path
            FinalContent = [string]$final
            Action       = $action
            TempPath     = $null
            BackupPath   = $null
        }) | Out-Null
    }

    if ($units.Count -eq 0) {
        Write-Output ""
        Write-Output ("[SUCCESS] All {0} blocks applied to {1} files." -f $TotalBlocks, 0)
        return
    }

    # Phase A: create any needed parent dirs for CREATE targets, then write temp files
    $createdDirs = New-Object System.Collections.Generic.List[string]
    try {
        foreach ($u in $units) {
            if ($u.Action -eq "create") {
                $dirs = Ensure-ParentDirsForCreate -TargetPath ([string]$u.TargetPath)
                foreach ($d in $dirs) {
                    $createdDirs.Add($d)
                }
            }

            if ($u.Action -in @("create", "update")) {
                $target = [System.IO.FileInfo]::new([string]$u.TargetPath)
                $parent = $target.Directory

                if ($parent -and -not $parent.Exists) {
                    throw ("Target directory does not exist: {0}" -f $parent.FullName)
                }

                $tempPath = New-UniqueSiblingPath -Target $target -Tag ".fsrb.tmp"
                Write-AllTextStrictUtf8 -Path $tempPath -Content ([string]$u.FinalContent)

                # Best-effort: preserve attributes (update only)
                if ($u.Action -eq "update") {
                    Try-CopyAttributesBestEffort -FromPath $target.FullName -ToPath $tempPath
                }

                $u.TempPath = $tempPath
            }
        }
    }
    catch {
        Cleanup-Leftovers -Units $units.ToArray()
        Remove-EmptyDirsBestEffort -Dirs $createdDirs.ToArray()
        throw
    }

    # Phase B: swap in with rollback
    $movedOriginals = New-Object System.Collections.Generic.List[string]
    $installedNews = New-Object System.Collections.Generic.List[string]

    try {
        foreach ($u in $units) {
            $targetPath = [string]$u.TargetPath

            if ($u.Action -eq "create") {
                # Ensure target does not exist (strict CREATE semantics)
                if ([System.IO.File]::Exists($targetPath)) {
                    throw ("CREATE target already exists: {0}" -f $targetPath)
                }

                [System.IO.File]::Move([string]$u.TempPath, $targetPath)
                $installedNews.Add($targetPath) | Out-Null
                continue
            }

            if ($u.Action -eq "delete") {
                # Move original out of the way; do not install a new file
                if (-not [System.IO.File]::Exists($targetPath)) {
                    throw ("DELETE target missing at commit time: {0}" -f $targetPath)
                }

                $targetFi = [System.IO.FileInfo]::new($targetPath)
                $backupPath = New-UniqueSiblingPath -Target $targetFi -Tag ".fsrb.bak"
                $u.BackupPath = $backupPath

                [System.IO.File]::Move($targetPath, $backupPath)
                $movedOriginals.Add($targetPath) | Out-Null
                continue
            }

            # update
            $targetFi = [System.IO.FileInfo]::new($targetPath)
            $backupPath = New-UniqueSiblingPath -Target $targetFi -Tag ".fsrb.bak"
            $u.BackupPath = $backupPath

            # Move original out of the way (atomic within same volume)
            [System.IO.File]::Move($targetPath, $backupPath)
            $movedOriginals.Add($targetPath) | Out-Null

            # Move temp into place
            [System.IO.File]::Move([string]$u.TempPath, $targetPath)
            $installedNews.Add($targetPath) | Out-Null
        }
    }
    catch {
        $rollbackErr = Rollback-Swaps -Units $units.ToArray() -InstalledNews $installedNews.ToArray() -MovedOriginals $movedOriginals.ToArray()
        Cleanup-Leftovers -Units $units.ToArray()
        Remove-EmptyDirsBestEffort -Dirs $createdDirs.ToArray()

        if ($rollbackErr) {
            throw ("Commit failed: {0}. Rollback also encountered errors: {1}" -f $_.Exception.Message, $rollbackErr)
        }
        throw
    }

    # Phase C: cleanup backups/temps
    Cleanup-Leftovers -Units $units.ToArray()

    # Post-commit: remove empty parent dirs for DELETE (best-effort)
    foreach ($u in $units) {
        if ($u.Action -eq "delete" -and -not [string]::IsNullOrEmpty($u.BackupPath)) {
            Remove-EmptyParentDirsBestEffort -FilePath ([string]$u.TargetPath)
        }
    }

    foreach ($u in ($units | Sort-Object -Property TargetPath)) {
        Write-Output ("Saved changes to {0}" -f [string]$u.TargetPath)
    }
    Write-Output ""
    Write-Output ("[SUCCESS] All {0} blocks applied to {1} files." -f $TotalBlocks, $units.Count)
}

# ---------------------------
# Main
# ---------------------------

try {
    if (-not (Test-Path -LiteralPath $PatchFile)) {
        Write-Error ("Error: Patch file '{0}' not found." -f $PatchFile)
        exit 1
    }

    # Resolve PS path to absolute FS path for .NET
    $ResolvedPath = (Resolve-Path -LiteralPath $PatchFile).Path

    $patchContent = $null
    try {
        $patchContent = Read-AllTextStrictUtf8 $ResolvedPath
    }
    catch {
        Write-Error ("Error reading patch file: {0}" -f $_.Exception.Message)
        exit 1
    }

    $patchBlocks = $null
    try {
        $patchBlocks = Parse-PatchFile -Content $patchContent
    }
    catch {
        Write-Error ("Error parsing patch file: {0}" -f $_.Exception.Message)
        exit 1
    }

    if (-not $patchBlocks -or $patchBlocks.Count -eq 0) {
        Write-Warning "No valid patch blocks found in file."
        exit 0
    }

    $result = Validate-AndApplyInMemory -Blocks $patchBlocks
    $fileStates = $result.FileStates
    $errors = $result.Errors

    if ($errors -and $errors.Count -gt 0) {
        Print-ErrorReport -Errors $errors
        exit 1
    }

    Commit-ChangesAtomic -FileStates $fileStates -TotalBlocks $patchBlocks.Count
    exit 0
}
catch {
    Write-Error ("CRITICAL ERROR: {0}" -f $_.Exception.Message)
    exit 1
}
