#!/usr/bin/env python3
"""latbot launcher: ``python run.py`` (forever) or ``python run.py 10`` (10 s smoke run).

start.bat / stop.bat / restart.bat wrap this file on Windows.
"""
from __future__ import annotations

import sys

from latbot.app import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
