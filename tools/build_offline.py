"""Переносит вопросы из questions.csv в офлайн-версию:  python tools/build_offline.py"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from questions import load_variants  # noqa: E402

sets, hard = load_variants("questions.csv")


def conv(board):
    return [{"name": c["name"], "q": [{"p": q["points"], "q": q["q"], "options": q["options"], "correct": q["correct"]}
                                       for q in c["questions"]]} for c in board]


path = os.path.join("offline", "index.html")
html = open(path, encoding="utf-8").read()
start = html.index("  var VARIANTS = ")
end = html.index("  var ML_INDEX = ")
variants = [conv(b) for b in sets] + ([conv(hard)] if hard else [])
ml_line_end = html.index("\n", end) + 1
ml_index = len(sets) if hard else -1
html = (html[:start] + "  var VARIANTS = " + json.dumps(variants, ensure_ascii=False) + ";\n"
        + f"  var ML_INDEX = {ml_index};\n" + html[ml_line_end:])
open(path, "w", encoding="utf-8").write(html)
print(f"offline/index.html: {len(sets)} вариантов" + (" + ML" if hard else ""))
