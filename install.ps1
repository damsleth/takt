# Install takt on Windows:
#   irm https://raw.githubusercontent.com/damsleth/takt/main/install.ps1 | iex
# Puts takt.py in ~\.local\share\takt\, writes ~\.local\bin\takt.cmd, and adds
# ~\.local\bin to the user PATH. $env:TAKT_REF pins a tag or commit instead of main.
# $env:TAKT_SOURCE installs a local file.
$ErrorActionPreference = 'Stop'

$ref = if ($env:TAKT_REF) { $env:TAKT_REF } else { 'main' }
$url = "https://raw.githubusercontent.com/damsleth/takt/$ref/takt.py"
$dir = Join-Path $env:USERPROFILE '.local\share\takt'
$bin = Join-Path $env:USERPROFILE '.local\bin'

# The py launcher finds a real interpreter. `python3` is often the Microsoft Store stub.
$py = $null
if (Get-Command py -ErrorAction SilentlyContinue) {
    $py = (& py -3 -c 'import sys; print(sys.executable)' 2>$null)
}
if (-not $py -and (Get-Command python -ErrorAction SilentlyContinue)) {
    $py = (& python -c 'import sys; print(sys.executable)' 2>$null)
}
if (-not $py) { throw 'takt needs Python 3.11 or later (install it from python.org).' }
& $py -c 'import sys; sys.exit(sys.version_info < (3, 11))'
if ($LASTEXITCODE -ne 0) { throw "takt needs Python 3.11 or later; found $(& $py -V)." }

New-Item -ItemType Directory -Force -Path $dir, $bin | Out-Null
$target = Join-Path $dir 'takt.py'
if ($env:TAKT_SOURCE) {
    Copy-Item $env:TAKT_SOURCE "$target.new" -Force
} else {
    Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile "$target.new"
}
Move-Item "$target.new" $target -Force
Set-Content -Path (Join-Path $bin 'takt.cmd') -Encoding ASCII -Value "@`"$py`" `"$target`" %*"

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (($userPath -split ';') -notcontains $bin) {
    [Environment]::SetEnvironmentVariable('Path', ($userPath.TrimEnd(';') + ";$bin").TrimStart(';'), 'User')
    $env:Path += ";$bin"
    Write-Host "added $bin to the user PATH (new terminals pick it up)"
}
Write-Host "installed $(& $py $target --version) to $bin\takt.cmd"
Write-Host "python for jobs.<host>.toml on this host: $py"
Write-Host 'next: takt init'
