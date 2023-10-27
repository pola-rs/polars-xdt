# type: ignore
import sys
import re
import subprocess

how = sys.argv[1]

subprocess.run(["cp", "../README.md", "polars_business/README.md"])
subprocess.run(["cp", "../LICENSE", "polars_business/LICENSE"])

with open("polars_business/pyproject.toml", "r", encoding="utf-8") as f:
    content = f.read()
old_version = re.search(r'version = "(.*)"', content).group(1)
version = old_version.split(".")
if how == "patch":
    version = ".".join(version[:-1] + [str(int(version[-1]) + 1)])
elif how == "minor":
    version = ".".join(version[:-2] + [str(int(version[-2]) + 1), "0"])
elif how == "major":
    version = ".".join([str(int(version[0]) + 1), "0", "0"])
content = content.replace(f'version = "{old_version}"', f'version = "{version}"')
with open("polars_business/pyproject.toml", "w", encoding="utf-8") as f:
    f.write(content)

with open("polars_business/polars_business/__init__.py", "r", encoding="utf-8") as f:
    content = f.read()
content = content.replace(
    f'__version__ = "{old_version}"', f'__version__ = "{version}"'
)
with open("polars_business/polars_business/__init__.py", "w", encoding="utf-8") as f:
    f.write(content)

subprocess.run(["git", "commit", "-a", "-m", f"Bump version to {version}"])
subprocess.run(["git", "tag", "-a", version, "-m", version])
subprocess.run(["git", "push", "--follow-tags"])
