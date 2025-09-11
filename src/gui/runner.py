from __future__ import annotations

import subprocess
import sys
import threading
from typing import Callable, List, Optional
from pathlib import Path
import os


class ProcessRunner:
    def __init__(self, on_line: Callable[[str], None]):
        self.on_line = on_line
        self._proc: Optional[subprocess.Popen] = None
        self._thread: Optional[threading.Thread] = None

    def run(self, args: List[str], cwd: Optional[Path] = None, on_exit: Optional[Callable[[int], None]] = None) -> None:
        if self._proc:
            self.on_line("[runner] A process is already running.")
            return

        def target():
            rc = -1
            try:
                self.on_line(f"[cmd] {' '.join(args)}")
                env = os.environ.copy()
                env["PYTHONUNBUFFERED"] = "1"
                env["PYTHONIOENCODING"] = "utf-8"
                self._proc = subprocess.Popen(
                    args,
                    cwd=str(cwd) if cwd else None,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    bufsize=1,
                    universal_newlines=True,
                    text=True,
                    env=env,
                )
                assert self._proc.stdout is not None
                for line in self._proc.stdout:
                    line = line.rstrip("\n")
                    if line:  # Skip empty lines
                        self.on_line(line)
                rc = self._proc.wait()
            except Exception as e:
                self.on_line(f"[error] {e}")
            finally:
                self._proc = None
                if on_exit:
                    on_exit(rc)

        self._thread = threading.Thread(target=target, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            try:
                self._proc.terminate()
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
        else:
            self.on_line("[runner] No active process.")


