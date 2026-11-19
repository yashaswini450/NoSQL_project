param(
    [ValidateSet('setup','download','mongodb','mapreduce','report','test','stop')]
    [string]$Action = 'report',
    [int]$BatchSize = 500000,
    [string]$RunId = ''
)
$ErrorActionPreference = 'Stop'
$linuxPath = (& wsl -d Ubuntu-24.04 --exec wslpath -a -u $PSScriptRoot).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Ubuntu-24.04 must be installed in WSL.' }
switch ($Action) {
    'setup' { & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash setup.sh }
    'stop' { & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash services.sh stop }
    'download' { & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash run.sh main.py download }
    'test' { & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash run.sh -m unittest discover -s tests -v }
    'report' {
        & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash services.sh start
        if ($LASTEXITCODE -ne 0) { throw 'Could not start databases.' }
        if ($RunId) { & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash run.sh reporter.py --run-id $RunId }
        else { & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash run.sh reporter.py }
    }
    default {
        & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash services.sh start
        if ($LASTEXITCODE -ne 0) { throw 'Could not start databases.' }
        & wsl -d Ubuntu-24.04 --cd $linuxPath --exec bash run.sh main.py run --pipeline $Action --batch-size $BatchSize
    }
}
if ($LASTEXITCODE -ne 0) { throw "Command failed with exit code $LASTEXITCODE" }
