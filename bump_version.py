# ruff: noqa
# type: ignore
import sys
import re
import subprocess
import os

# Docs are in a a separate repo cause I couldn't figure out
# how to deploy from readthedocs
subprocess.run(["make", "install", "--release"])
subprocess.run(["make", "clean"], cwd="docs")
subprocess.run(["make", "html"], cwd="docs")
os.system("cp docs/_build/html/* ../docs-polars-xdt/ -r")
subprocess.run(["git", "add", "."], cwd="../docs-polars-xdt")
subprocess.run(
    ["git", "commit", "-m", '"new version"', "--allow-empty"],
    cwd="../docs-polars-xdt",
)
subprocess.run(["git", "push", "origin", "HEAD"], cwd="../docs-polars-xdt")

how = sys.argv[1]

with open("Cargo.toml", "r", encoding="utf-8") as f:
    content = f.read()
old_version = re.search(r'(?<! )version = "(.*)"', content).group(1)
version = old_version.split(".")
if how == "patch":
    version = ".".join(version[:-1] + [str(int(version[-1]) + 1)])
elif how == "minor":
    version = ".".join(version[:-2] + [str(int(version[-2]) + 1), "0"])
elif how == "major":
    version = ".".join([str(int(version[0]) + 1), "0", "0"])
else:
    sys.exit(1)
content = content.replace(
    f'version = "{old_version}"', f'version = "{version}"'
)
with open("Cargo.toml", "w", encoding="utf-8") as f:
    f.write(content)

subprocess.run(["git", "commit", "-a", "-m", f"Bump version to {version}"])
subprocess.run(["git", "tag", "-a", version, "-m", version])
subprocess.run(["git", "push", "origin", "HEAD", "--follow-tags"])
