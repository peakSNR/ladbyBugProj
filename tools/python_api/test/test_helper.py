import sys
from pathlib import Path

MONAD_ROOT = Path(__file__).parent.parent.parent.parent

if sys.platform == "win32":
    # \ in paths is not supported by monad's parser
    MONAD_ROOT = str(MONAD_ROOT).replace("\\", "/")
