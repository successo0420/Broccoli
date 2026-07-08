Updating your package is a quick loop: you bump the version, clean out your old build files, rebuild, and push.

PyPI is strictly "append-only" — you cannot overwrite an existing version of your package. You must increment the version number every time you upload.

Here is the exact process:

1. **Bump the Version Number:**
Open your `pyproject.toml` and find the `version` field. Increase it to your new version (e.g., using semantic versioning: `0.1.1` for bug fixes, `0.2.0` for new features).

```toml
[project]
name = "broccoli-queue" 
version = "0.1.1"  # ← Must be higher than the last uploaded version

```


2. **Delete the Old Build:** Prevents upload conflicts.
Before building, delete the existing `dist/` folder in your `FAKECELERY` directory. If you skip this, `twine` will try to upload both the old `0.1.0` files and your new `0.1.1` files, and PyPI will reject the entire batch because `0.1.0` already exists.

* **Mac/Linux:** `rm -rf dist/`
* **Windows (PowerShell):** `Remove-Item -Recurse -Force dist\`


3. **Build the New Version:**
Run the build command exactly as you did the first time:

```bash
python -m build

```

This will generate a fresh `dist/` folder containing only your new version's `.whl` and `.tar.gz` files.


4. **Upload to PyPI:**
Push the new files to PyPI:

```bash
twine upload dist/*

```


Once the upload finishes, users can immediately pull the newest code by running `pip install --upgrade <your-package-name>`.