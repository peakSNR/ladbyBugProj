import os
import shutil
import subprocess
import sys

MONAD_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
# Datasets can only be copied from the root since copy.schema contains relative paths
os.chdir(MONAD_ROOT)

# Define the build type from input
if len(sys.argv) > 1 and sys.argv[1].lower() == "release":
    build_type = "release"
else:
    build_type = "relwithdebinfo"

# Change the current working directory
if os.path.exists(f"{MONAD_ROOT}/dataset/databases/tinysnb"):
    shutil.rmtree(f"{MONAD_ROOT}/dataset/databases/tinysnb")
if sys.platform == "win32":
    monad_shell_path = f"{MONAD_ROOT}/build/{build_type}/src/monad_shell"
else:
    monad_shell_path = f"{MONAD_ROOT}/build/{build_type}/tools/shell/monad"
subprocess.check_call(
    [
        "python3",
        f"{MONAD_ROOT}/benchmark/serializer.py",
        "TinySNB",
        f"{MONAD_ROOT}/dataset/tinysnb",
        f"{MONAD_ROOT}/dataset/databases/tinysnb",
        "--single-thread",
        "--monad-shell",
        monad_shell_path,
    ]
)
