# publish.ps1
Write-Host "🚀 Starting PyPI publish process..." -ForegroundColor Cyan

# 1. Upgrade build tools
python -m pip install --upgrade build twine

# 2. Clean up old builds
if (Test-Path dist) {
    Remove-Item -Recurse -Force dist
}

# 3. Build the package
python -m build

# 4. Upload to PyPI
twine upload dist/*

Write-Host "🎉 Package published successfully!" -ForegroundColor Green