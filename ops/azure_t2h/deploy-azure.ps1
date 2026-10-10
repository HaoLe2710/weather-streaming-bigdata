[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9.@:_-]{0,254}$')]
    [string] $SshHost,

    [string] $IdentityFile,

    [ValidatePattern('^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$')]
    [string] $Ref = 'deploy/azure-phase17',

    [switch] $Apply,

    [switch] $RunFreshReadiness
)

$ErrorActionPreference = 'Stop'
if ($RunFreshReadiness -and -not $Apply) {
    throw '-RunFreshReadiness requires -Apply so code deployment and readiness are explicit.'
}
if ($Ref.Contains('..') -or $Ref.EndsWith('/')) {
    throw 'Unsafe Git ref.'
}
if ($IdentityFile -and -not (Test-Path -LiteralPath $IdentityFile -PathType Leaf)) {
    throw "SSH identity file does not exist: $IdentityFile"
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$bootstrapPath = Join-Path $PSScriptRoot 'bootstrap-azure.sh'
$bootstrap = Get-Content -LiteralPath $bootstrapPath -Raw
$sshArgs = @(
    '-T',
    '-o', 'BatchMode=yes',
    '-o', 'StrictHostKeyChecking=yes'
)
if ($IdentityFile) {
    $sshArgs += @('-i', (Resolve-Path -LiteralPath $IdentityFile).Path, '-o', 'IdentitiesOnly=yes')
}

$applyText = if ($Apply) { 'true' } else { 'false' }
$readinessText = if ($RunFreshReadiness) { 'true' } else { 'false' }
$remoteArgs = @($Ref, $applyText, $readinessText) | ForEach-Object { "'$_'" }
$remoteCommand = 'bash -s -- ' + ($remoteArgs -join ' ')

Write-Host "Connecting to $SshHost with strict SSH host-key checking."
Write-Host "Remote target: /opt/weather-streaming/weather-streaming-bigdata"
Write-Host "Ref: $Ref; apply: $applyText; Fresh Formal Readiness: $readinessText"
Write-Host 'The SSH private key is read from the supplied path or SSH agent and is never copied to the repository.'

$bootstrap | & ssh @sshArgs $SshHost $remoteCommand
if ($LASTEXITCODE -ne 0) {
    throw "Azure deployment command failed with exit code $LASTEXITCODE."
}
