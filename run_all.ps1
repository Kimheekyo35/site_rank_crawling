$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

$jobs = @(
    Start-Job -Name "jolse" -ScriptBlock {
        param($dir)
        Set-Location $dir
        python jolse_test.py
    } -ArgumentList $root
    Start-Job -Name "stylevana" -ScriptBlock {
        param($dir)
        Set-Location $dir
        python stylevana_crawling.py
    } -ArgumentList $root
    Start-Job -Name "yesstyle" -ScriptBlock {
        param($dir)
        Set-Location $dir
        python yesstyle_crawling.py
    } -ArgumentList $root
)

Wait-Job -Job $jobs
foreach ($job in $jobs) {
    Write-Host "=== $($job.Name) output ==="
    Receive-Job -Job $job
    if ($job.State -ne 'Completed') {
        Write-Warning "$($job.Name) finished with state $($job.State)"
    }
}
Remove-Job -Job $jobs
