"""``python -m latbot [SECONDS]`` - run the terminal (see latbot.app.main)."""
from __future__ import annotations

import sys

from .app import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
