"""Проверка таблиц с вопросами перед запуском:  python tools/check_questions.py

Проверяет questions.csv и (если есть) live_questions.csv: структуру каждого варианта
и то, что вопросы трансляции не повторяют вопросы из обычных вариантов.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from questions import QuestionsError, load_single_board, load_variants, read_text  # noqa: E402

ok = True
try:
    sets, hard = load_variants("questions.csv")
    print(f"questions.csv: {len(sets)} вариантов по 25 вопросов" + (" + ML-хардкор" if hard else ""))
except QuestionsError as e:
    print("questions.csv — ОШИБКИ:\n" + str(e))
    sys.exit(1)

public = {q["q"].strip().lower() for board in sets + ([hard] if hard else []) for c in board for q in c["questions"]}
if len(public) < 25 * (len(sets) + (1 if hard else 0)):
    print("!  В questions.csv есть повторяющиеся вопросы")
    ok = False

if os.path.exists("live_questions.csv"):
    try:
        live = load_single_board(read_text("live_questions.csv"), "live_questions.csv")
        print("live_questions.csv: 25 вопросов трансляции")
        files = {q[k] for c in live for q in c["questions"] for k in ("media", "media_answer") if q[k] and not q[k].startswith("https://")}
        missing = sorted(f for f in files if not os.path.exists(os.path.join("media", f)))
        if files:
            print(f"  картинок и видео из папки media/: {len(files)}")
        if missing:
            ok = False
            print("!  Этих файлов нет в папке media/: " + ", ".join(missing))
        dup = [q["q"] for c in live for q in c["questions"] if q["q"].strip().lower() in public]
        if dup:
            ok = False
            print("!  Эти вопросы трансляции есть и в обычных вариантах (набор должен быть уникальным):")
            for d in dup:
                print("   -", d)
    except QuestionsError as e:
        print("live_questions.csv — ОШИБКИ:\n" + str(e))
        sys.exit(1)
else:
    print("live_questions.csv нет — трансляция пойдёт на одном из обычных вариантов (LIVE_VARIANT)")

print("Всё в порядке" if ok else "Есть замечания — см. выше")
sys.exit(0 if ok else 1)
