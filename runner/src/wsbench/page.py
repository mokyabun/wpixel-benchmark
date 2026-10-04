"""Session report page: report.html next to summary.json, with one chart per
metric. Self-contained (the template draws inline SVG), so it opens straight
from the results directory with no server and no network."""

import json
from pathlib import Path

TEMPLATE = Path(__file__).with_name("report.html")


def write_html(session: Path, rows: list[dict], capacity: list[dict]) -> None:
    meta_file = session / "meta.json"
    meta = json.loads(meta_file.read_text()) if meta_file.exists() else {}
    data = json.dumps({"session": session.name, "meta": meta, "rows": rows, "capacity": capacity})
    page = TEMPLATE.read_text().replace("/*DATA*/null", data.replace("</", "<\\/"))
    (session / "report.html").write_text(page)
