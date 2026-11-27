param(
    [switch]$RequireDatabase,
    [switch]$RequirePython
)

$ErrorActionPreference = 'Stop'

function Invoke-Check {
    param([string]$Name, [scriptblock]$Body)
    Write-Host "==> $Name"
    & $Body
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
}

Invoke-Check 'format' { cargo fmt --all -- --check }
Invoke-Check 'clippy (all features)' {
    cargo clippy --workspace --all-targets --all-features -- -D warnings
}
Invoke-Check 'tests (default features)' { cargo test --workspace --all-targets }
Invoke-Check 'tests (experimental RPC)' { cargo test --features experimental-rpc --lib }

if ($env:ACTOR_TEST_DATABASE_URL) {
    Invoke-Check 'tests (all features, PostgreSQL)' {
        cargo test --workspace --all-targets --all-features -- --test-threads=1
    }
} elseif ($RequireDatabase) {
    throw 'ACTOR_TEST_DATABASE_URL is required by -RequireDatabase'
} else {
    Write-Host 'SKIP PostgreSQL integration: ACTOR_TEST_DATABASE_URL is unset'
}

function Resolve-Python {
    foreach ($name in @('python3', 'python')) {
        $candidate = Get-Command $name -ErrorAction SilentlyContinue
        if (-not $candidate -or -not (Test-Path -LiteralPath $candidate.Source)) {
            continue
        }
        try {
            $global:LASTEXITCODE = 0
            & $candidate.Source --version *> $null
            if ($LASTEXITCODE -eq 0) {
                return $candidate.Source
            }
        } catch {
            continue
        }
    }
    return $null
}

$python = Resolve-Python

if ($python) {
    Invoke-Check 'SQLite protocol check' { & $python checks/sqlite_protocol_check.py }
    Invoke-Check 'cluster lease protocol check' { & $python checks/cluster_lease_check.py }
} elseif ($RequirePython) {
    throw 'Python is required by -RequirePython'
} else {
    Write-Host 'SKIP protocol checks: no Python interpreter found'
}

Invoke-Check 'documentation' {
    $env:RUSTDOCFLAGS = '-D rustdoc::broken_intra_doc_links'
    cargo doc --workspace --no-deps --all-features
}

Write-Host 'Actryn validation complete'
