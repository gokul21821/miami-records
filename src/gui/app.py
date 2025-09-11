from __future__ import annotations

import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter.scrolledtext import ScrolledText
from pathlib import Path

from .runner import ProcessRunner
from .paths import (
    ROOT_DIR,
    months_in_range, discover_available_months,
    normalized_csv_path, normalized_clean_csv_path, enriched_csv_path,
    pick_enrichment_input,
)
from .state import load_state, save_state


DEFAULT_COOKIES = (
    "NSC_JOeqtbnye4rqvqae52yysbdjdcwntcw=7ce2a3d93287e39e0a3142520a74f0b88d9f176cdcf72de67d2df59bf583b8a94149188e; "
    ".PremierIDDade=hStXCTj14zaDgObXFky4Bw%3D%3D"
)


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Miami Mortgage Enrichment")
        self.geometry("1000x700")
        self._busy = False

        self.state = load_state()
        self.runner = ProcessRunner(self._append_log)

        self._build_ui()
        self._load_state_defaults()
        self._refresh_months()

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        nb = ttk.Notebook(self)
        nb.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        self.fetch_tab = ttk.Frame(nb)
        self.csv_tab = ttk.Frame(nb)
        self.enrich_tab = ttk.Frame(nb)

        nb.add(self.fetch_tab, text="Fetch Records")
        nb.add(self.csv_tab, text="Create CSV")
        nb.add(self.enrich_tab, text="Enrich Phones")

        self._build_fetch_tab()
        self._build_csv_tab()
        self._build_enrich_tab()

        bottom = ttk.Frame(self)
        bottom.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
        bottom.columnconfigure(0, weight=1)
        bottom.rowconfigure(0, weight=1)

        self.log = ScrolledText(bottom, height=20, wrap="word")
        self.log.grid(row=0, column=0, sticky="nsew")

        controls = ttk.Frame(bottom)
        controls.grid(row=0, column=1, sticky="ns", padx=(8, 0))
        self.stop_btn = ttk.Button(controls, text="Stop", command=self._stop_proc)
        self.stop_btn.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.clear_btn = ttk.Button(controls, text="Clear Logs", command=lambda: self.log.delete(1.0, tk.END))
        self.clear_btn.grid(row=1, column=0, sticky="ew")

    def _build_fetch_tab(self) -> None:
        f = self.fetch_tab
        for i in range(4):
            f.columnconfigure(i, weight=1)

        ttk.Label(f, text="Start Date (YYYY-MM-DD)").grid(row=0, column=0, sticky="w")
        self.f_start = ttk.Entry(f)
        self.f_start.grid(row=1, column=0, sticky="ew", padx=(0, 8))

        ttk.Label(f, text="End Date (YYYY-MM-DD)").grid(row=0, column=1, sticky="w")
        self.f_end = ttk.Entry(f)
        self.f_end.grid(row=1, column=1, sticky="ew", padx=(0, 8))

        ttk.Label(f, text="Cookies").grid(row=0, column=2, sticky="w")
        self.f_cookies = ttk.Entry(f)
        self.f_cookies.grid(row=1, column=2, columnspan=2, sticky="ew")

        self.f_force = tk.BooleanVar(value=False)
        ttk.Checkbutton(f, text="Force reprocess", variable=self.f_force).grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Label(f, text="Reprocess dates already fetched", font=("TkDefaultFont", 8), foreground="gray").grid(row=3, column=0, sticky="w", pady=(0, 8))

        self.fetch_btn = ttk.Button(f, text="Fetch Records", command=self._on_fetch)
        self.fetch_btn.grid(row=2, column=3, sticky="e", pady=(8, 0))

    def _build_csv_tab(self) -> None:
        f = self.csv_tab
        for i in range(4):
            f.columnconfigure(i, weight=1)

        ttk.Label(f, text="Start Date (YYYY-MM-DD)").grid(row=0, column=0, sticky="w")
        self.c_start = ttk.Entry(f)
        self.c_start.grid(row=1, column=0, sticky="ew", padx=(0, 8))

        ttk.Label(f, text="End Date (YYYY-MM-DD)").grid(row=0, column=1, sticky="w")
        self.c_end = ttk.Entry(f)
        self.c_end.grid(row=1, column=1, sticky="ew", padx=(0, 8))

        self.c_force = tk.BooleanVar(value=True)
        ttk.Checkbutton(f, text="Force rebuild", variable=self.c_force).grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Label(f, text="Rebuild CSVs even if they exist", font=("TkDefaultFont", 8), foreground="gray").grid(row=3, column=0, sticky="w", pady=(0, 8))

        self.csv_btn = ttk.Button(f, text="Create CSV (normalize + dedupe)", command=self._on_csv_pipeline)
        self.csv_btn.grid(row=2, column=3, sticky="e", pady=(8, 0))

    def _build_enrich_tab(self) -> None:
        f = self.enrich_tab
        for i in range(6):
            f.columnconfigure(i, weight=1)
        f.rowconfigure(2, weight=1)  # Allow expansion for helper text

        ttk.Label(f, text="Month").grid(row=0, column=0, sticky="w")
        self.month_var = tk.StringVar(value="")
        self.month_combo = ttk.Combobox(f, textvariable=self.month_var, values=[], state="readonly")
        self.month_combo.grid(row=1, column=0, sticky="ew", padx=(0, 8))

        self.refresh_months_btn = ttk.Button(f, text="Refresh Months", command=self._refresh_months)
        self.refresh_months_btn.grid(row=1, column=1, sticky="w")

        ttk.Label(f, text="From row (1-based)").grid(row=0, column=2, sticky="w")
        self.e_from = ttk.Entry(f)
        self.e_from.grid(row=1, column=2, sticky="ew", padx=(8, 8))

        ttk.Label(f, text="To row (1-based)").grid(row=0, column=3, sticky="w")
        self.e_to = ttk.Entry(f)
        self.e_to.grid(row=1, column=3, sticky="ew")

        ttk.Label(f, text="Sleep sec").grid(row=0, column=4, sticky="w")
        self.e_sleep = ttk.Entry(f)
        self.e_sleep.grid(row=1, column=4, sticky="ew", padx=(8, 8))
        self.e_sleep.insert(0, "1.0")

        self.e_refresh = tk.BooleanVar(value=False)
        ttk.Checkbutton(f, text="Refresh cache", variable=self.e_refresh).grid(row=0, column=5, sticky="w")

        # Helper text for checkboxes
        ttk.Label(f, text="Ignores cached phone lookups", font=("TkDefaultFont", 8), foreground="gray").grid(row=1, column=5, sticky="w", padx=(0, 8))

        self.enrich_btn = ttk.Button(f, text="Enrich Phones", command=self._on_enrich)
        self.enrich_btn.grid(row=2, column=5, sticky="e", pady=(8, 0))

    def _on_fetch(self) -> None:
        if self._busy:
            return
        start = self.f_start.get().strip()
        end = self.f_end.get().strip()
        cookies = self.f_cookies.get().strip() or DEFAULT_COOKIES
        if not start or not end:
            messagebox.showerror("Validation", "Please enter start and end dates.")
            return

        args = [
            sys.executable, "-u", "-m", "src.miami_mor_step2",
            "--start-date", start,
            "--end-date", end,
            "--mode", "auto",
            "--cookies", cookies,
        ]
        if self.f_force.get():
            args.append("--force")

        self._run(args, label=f"Fetch {start}..{end}", after=lambda rc: self._refresh_months())

    def _on_csv_pipeline(self) -> None:
        if self._busy:
            return
        start = self.c_start.get().strip()
        end = self.c_end.get().strip()
        if not start or not end:
            messagebox.showerror("Validation", "Please enter start and end dates.")
            return

        months = months_in_range(start, end)
        if not months:
            messagebox.showerror("Validation", "No months in selected range.")
            return

        step3 = [
            sys.executable, "-u", "-m", "src.miami_mor_step3",
            "--start-date", start,
            "--end-date", end,
        ]
        if self.c_force.get():
            step3.append("--force")

        def after_step3(rc: int) -> None:
            if rc != 0:
                return
            for m in months:
                inp = normalized_csv_path(m)
                if not inp.exists():
                    self._append_log(f"[dedupe] Skipping {m} (no normalized CSV at {inp})")
                    continue
                outp = normalized_clean_csv_path(m)
                args = [sys.executable, "-u", str(ROOT_DIR / "src" / "remove_duplicates.py"), str(inp), str(outp)]
                self._append_log(f"[dedupe] Running dedupe for {m}")
                done = threading.Event()

                def _on_exit(_rc: int) -> None:
                    self._append_log(f"[dedupe] Done {m} rc={_rc}")
                    done.set()

                self.runner.run(args, cwd=ROOT_DIR, on_exit=_on_exit)
                done.wait()
            self._refresh_months()

        self._run(step3, label=f"Create CSV {start}..{end}", after=after_step3)

    def _on_enrich(self) -> None:
        if self._busy:
            return
        month = self.month_var.get().strip()
        if not month:
            messagebox.showerror("Validation", "Please select a month.")
            return

        src_path, reason = pick_enrichment_input(month)
        if src_path is None:
            messagebox.showerror("Input missing", f"No normalized CSV found for {month}.\nExpected:\n{normalized_clean_csv_path(month)}\nor\n{normalized_csv_path(month)}")
            return
        dst_path = enriched_csv_path(month)
        self._append_log(f"[enrich] Using input ({reason}): {src_path}")
        self._append_log(f"[enrich] Output: {dst_path}")

        args = [sys.executable, "-u", str(ROOT_DIR / "run.py")]
        from_row = self.e_from.get().strip()
        to_row = self.e_to.get().strip()
        if from_row:
            args += ["--from-row", from_row]
        if to_row:
            args += ["--to-row", to_row]

        sleep = self.e_sleep.get().strip() or "1.0"
        args += ["--sleep-sec", sleep]
        if self.e_refresh.get():
            args.append("--refresh")

        args += [str(src_path), str(dst_path)]

        self._run(args, label=f"Enrich {month}")

    def _run(self, args, label: str, after=None) -> None:
        if self._busy:
            return
        self._busy = True
        self._set_controls_enabled(False)
        self._append_log(f"=== {label} START ===")

        def on_exit(rc: int) -> None:
            self._append_log(f"=== {label} END (rc={rc}) ===")
            self._busy = False
            self._set_controls_enabled(True)
            if after:
                after(rc)

        self.runner.run(args, cwd=ROOT_DIR, on_exit=on_exit)

    def _stop_proc(self) -> None:
        self.runner.stop()

    def _append_log(self, line: str) -> None:
        self.log.insert(tk.END, line + "\n")
        self.log.see(tk.END)
        self.update_idletasks()  # Force GUI update for live streaming

    def _set_controls_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        for w in [
            self.fetch_btn, self.csv_btn, self.enrich_btn,
            self.stop_btn, self.clear_btn, self.refresh_months_btn,
            self.f_start, self.f_end, self.f_cookies,
            self.c_start, self.c_end,
            self.month_combo, self.e_from, self.e_to, self.e_sleep
        ]:
            try:
                w.configure(state=state)
            except Exception:
                pass

    def _load_state_defaults(self) -> None:
        self.f_cookies.insert(0, self.state.get("cookies", DEFAULT_COOKIES))
        self.f_start.insert(0, self.state.get("fetch_start", "2025-01-01"))
        self.f_end.insert(0, self.state.get("fetch_end", "2025-01-31"))

        self.c_start.insert(0, self.state.get("csv_start", "2025-01-01"))
        self.c_end.insert(0, self.state.get("csv_end", "2025-01-31"))

        self.e_sleep.delete(0, tk.END)
        self.e_sleep.insert(0, str(self.state.get("sleep_sec", "1.0")))
        last_month = self.state.get("month", "")
        if last_month:
            self.month_var.set(last_month)

    def _refresh_months(self) -> None:
        months = discover_available_months()
        self.month_combo["values"] = months
        if months and not self.month_var.get():
            self.month_var.set(months[-1])
        self.state["cookies"] = self.f_cookies.get().strip()
        self.state["fetch_start"] = self.f_start.get().strip()
        self.state["fetch_end"] = self.f_end.get().strip()
        self.state["csv_start"] = self.c_start.get().strip()
        self.state["csv_end"] = self.c_end.get().strip()
        self.state["sleep_sec"] = self.e_sleep.get().strip()
        self.state["month"] = self.month_var.get().strip()
        save_state(self.state)


def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()


