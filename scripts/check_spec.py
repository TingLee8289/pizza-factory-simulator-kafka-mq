"""docs/spec 一致性檢查(僅用標準函式庫)。

檢查 PRD/FRD/SRD/AT 的 ID 定義、REQ->FR->AT 覆蓋、引用存在、Phase 有效與必要欄位。
追溯矩陣(AT.md 中標題含「追溯矩陣」之後的內容)被排除在定義與覆蓋邏輯之外。
用法: uv run python scripts/check_spec.py [--root DIR]
DIR 需含 docs/spec/*.md 與 .planning/ROADMAP.md(預設為本 repo 根目錄)。
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

REQ = r"(?:INFRA|ORDER|STATION|SCHED|FAIL|KPI|DASH|COMP|ADV|TOOL)-\d{2}"
ID_RE = re.compile(
    rf"\b(?:{REQ}|FR-[A-Z]+-\d{{2}}|SYS-\d{{2}}|NFR-\d{{2}}|AT-\d{{2}})\b"
)
FIELD_KEYS = ("Verifies", "Phase", "Method", "Given", "When", "Then", "Satisfies", "Refines", "Description")
FIELD_KEY_RE = re.compile(r"^(" + "|".join(FIELD_KEYS) + r"):\s*(.*)$")

DEF_RE = re.compile(
    rf"^- \*\*({REQ}|FR-[A-Z]+-\d{{2}}|SYS-\d{{2}}|NFR-\d{{2}}|AT-\d{{2}})\*\*"
)
FILES = ("PRD", "FRD", "SRD", "AT")
METHODS = {"自動測試", "手動", "KPI 對照 IE 理論值"}


def kind(i: str) -> str:
    if i.startswith("FR-"):
        return "FR"
    if i.startswith("SYS-"):
        return "SYS"
    if i.startswith("NFR-"):
        return "NFR"
    if i.startswith("AT-"):
        return "AT"
    return "REQ"


def strip_matrix(name: str, text: str) -> str:
    if name != "AT":
        return text
    out = []
    for line in text.splitlines():
        if line.startswith("#") and "追溯矩陣" in line:
            break
        out.append(line)
    return "\n".join(out)


def parse(text: str):
    """Return list of (id, line_no, fields{key: value}) for definitions, and refs."""
    defs = []
    cur = None
    for n, line in enumerate(text.splitlines(), 1):
        s = line.strip()
        if s.startswith("|") and "**" in s:
            cells = [c.strip() for c in s.strip("|").split("|")]
            m0 = re.search(
                r"\*\*((?:INFRA|ORDER|STATION|SCHED|FAIL|KPI|DASH|COMP|ADV|TOOL)-\d{2}"
                r"|FR-[A-Z]+-\d{2}|SYS-\d{2}|NFR-\d{2}|AT-\d{2})\*\*",
                cells[0] if cells else "",
            )
            if m0 and len(cells) >= 3:
                i = m0.group(1)
                if len(cells) >= 4:
                    entry = (i, n, {"Description": cells[1], "Satisfies": cells[2], "Refines": cells[2], "Phase": cells[3]})
                else:
                    entry = (i, n, {"Description": cells[1], "Satisfies": cells[2], "Refines": cells[2]})
                defs.append(entry)
                cur = None
                continue
        m = DEF_RE.match(line)
        if m:
            cur = (m.group(1), n, {})
            defs.append(cur)
            continue
        if line.startswith("#"):
            cur = None
            continue
        f = re.match(r"^\s+([A-Za-z]+):\s*(.*)$", line) or FIELD_KEY_RE.match(line)
        if f and cur:
            cur[2][f.group(1)] = f.group(2).strip()
            continue
        if cur and s.startswith("|"):
            cells = [c.strip() for c in s.strip("|").split("|")]
            if len(cells) == 2 and cells[0] in (
                "Verifies", "Phase", "Method", "Given", "When", "Then", "Satisfies", "Refines",
            ):
                cur[2][cells[0]] = cells[1]
    return defs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    root = Path(ap.parse_args().root)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    problems: dict[str, list[str]] = defaultdict(list)

    texts = {}
    for name in FILES:
        p = root / "docs" / "spec" / f"{name}.md"
        if not p.exists():
            problems["(g) missing spec file"].append(str(p))
        else:
            texts[name] = strip_matrix(name, p.read_text(encoding="utf-8"))

    rm = root / ".planning" / "ROADMAP.md"
    phases = set()
    if rm.exists():
        phases = set(re.findall(r"^### Phase (\d+(?:\.\d+)?):", rm.read_text(encoding="utf-8"), re.M))
    else:
        problems["(g) missing spec file"].append(str(rm))

    defined: dict[str, str] = {}
    fields: dict[str, dict[str, str]] = {}
    for name, text in texts.items():
        for i, n, f in parse(text):
            if i in defined:
                problems["(a) duplicate definition"].append(f"{i} in {name}.md:{n} (first in {defined[i]})")
            else:
                defined[i] = f"{name}.md:{n}"
                fields[i] = f

    # (d) references exist
    for name, text in texts.items():
        for n, line in enumerate(text.splitlines(), 1):
            for ref in ID_RE.findall(line):
                if ref not in defined:
                    problems["(d) undefined reference"].append(f"{ref} at {name}.md:{n}")

    def ids_in(value: str) -> set[str]:
        return set(ID_RE.findall(value))

    satisfied: set[str] = set()
    verified: set[str] = set()
    for i, f in fields.items():
        k = kind(i)
        if k == "FR":
            satisfied |= ids_in(f.get("Satisfies", ""))
        if k == "AT":
            verified |= ids_in(f.get("Verifies", ""))

    for i, f in sorted(fields.items()):
        k = kind(i)
        if k in ("REQ", "FR", "AT"):
            ph = f.get("Phase")
            if ph is None:
                problems["(f) missing field"].append(f"{i}: Phase")
            elif ph != "deferred" and ph not in phases:
                problems["(e) invalid Phase"].append(f"{i}: Phase {ph!r} not in ROADMAP")
        if k == "REQ" and f.get("Phase") != "deferred" and i not in satisfied:
            problems["(b) REQ without FR"].append(i)
        if k == "FR":
            if "Satisfies" not in f or not ids_in(f["Satisfies"]):
                problems["(f) missing field"].append(f"{i}: Satisfies")
            if i not in verified:
                problems["(c) FR without AT"].append(i)
        if k == "AT":
            for key in ("Verifies", "Method", "Given", "When", "Then"):
                if not f.get(key):
                    problems["(f) missing field"].append(f"{i}: {key}")
            m = f.get("Method")
            if m and m not in METHODS:
                problems["(f) invalid Method"].append(f"{i}: {m!r}")

    counts = defaultdict(int)
    for i in defined:
        counts[kind(i)] += 1
    summary = ", ".join(f"{k}={counts[k]}" for k in ("REQ", "FR", "SYS", "NFR", "AT"))
    if problems:
        for group, items in sorted(problems.items()):
            print(f"[FAIL] {group}")
            for it in items:
                print(f"  - {it}")
        print(f"\n{sum(map(len, problems.values()))} problem(s). {summary}")
        return 1
    print(f"OK: docs/spec consistent. {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
