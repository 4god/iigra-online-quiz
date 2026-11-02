"""
Загрузка вопросов из CSV-таблиц — редактировать их можно прямо в Excel.

  questions.csv       обычные варианты (1, 2, 3, …) и пасхальный «ML» — лежит в репозитории
  live_questions.csv  вопросы трансляции — только у организатора, в git не попадает

Столбцы (порядок любой, разделитель «;» или «,», кодировка UTF-8 или Windows-1251):
  Вариант | Тема | Очки | Вопрос | Правильный ответ | Неверный 1 | Неверный 2 | Неверный 3

В каждом варианте 5 тем, в каждой теме по одному вопросу на 100, 200, 300, 400 и 500 очков.
Порядок ответов на экране перемешивается автоматически.
"""

import csv
import io
import random
from pathlib import Path

BASE_DIR = Path(__file__).parent
POINTS = [100, 200, 300, 400, 500]
N_THEMES = 5
COLUMNS = ["Вариант", "Тема", "Очки", "Вопрос", "Правильный ответ", "Неверный 1", "Неверный 2", "Неверный 3"]


class QuestionsError(Exception):
    pass


def _question(points, raw):
    text, correct, *wrong = raw
    options = [correct, *wrong]
    random.Random(text).shuffle(options)  # stable: the same order on every restart
    return {"points": points, "q": text, "options": options, "correct": options.index(correct)}


def read_text(path):
    data = Path(path).read_bytes()
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("cp1251")  # «CSV (разделители — запятые)» from Russian Excel on Windows


def parse_csv(text, source):
    """Returns {variant label: board}; board = [{"name": theme, "questions": [5 questions by points]}] * 5."""
    first = text.split("\n", 1)[0]
    delim = max([";", ",", "\t"], key=first.count)
    rows = list(csv.reader(io.StringIO(text), delimiter=delim))
    if not rows:
        raise QuestionsError(f"{source}: файл пустой")

    header = [h.strip().lower() for h in rows[0]]
    missing = [c for c in COLUMNS if c.lower() not in header]
    if missing:
        raise QuestionsError(f"{source}: нет столбцов {', '.join(missing)}. Первая строка должна быть: {'; '.join(COLUMNS)}")
    idx = {c: header.index(c.lower()) for c in COLUMNS}

    errors = []
    variants = {}
    for n, row in enumerate(rows[1:], start=2):
        if not any(cell.strip() for cell in row):
            continue

        def cell(c):
            return row[idx[c]].strip() if idx[c] < len(row) else ""

        where = f"{source}, строка {n}"
        label = cell("Вариант")
        label = "ML" if label.lower() == "ml" else label
        try:
            points = int(float(cell("Очки").replace(",", ".")))
        except ValueError:
            errors.append(f"{where}: «Очки» должны быть числом 100–500, а не «{cell('Очки')}»")
            continue
        if points not in POINTS:
            errors.append(f"{where}: «Очки» — одно из {', '.join(map(str, POINTS))}, а не {points}")
            continue
        text, theme = cell("Вопрос"), cell("Тема")
        options = [cell("Правильный ответ"), cell("Неверный 1"), cell("Неверный 2"), cell("Неверный 3")]
        if not label:
            errors.append(f"{where}: не заполнен «Вариант»")
            continue
        if not theme or not text:
            errors.append(f"{where}: не заполнены «Тема» или «Вопрос»")
            continue
        if not all(options):
            errors.append(f"{where}: нужны правильный ответ и три неверных")
            continue
        if len({o.lower() for o in options}) < 4:
            errors.append(f"{where}: варианты ответа повторяются")
            continue
        themes = variants.setdefault(label, {})
        slot = themes.setdefault(theme, {})
        if points in slot:
            errors.append(f"{where}: в варианте «{label}», теме «{theme}» уже есть вопрос на {points}")
            continue
        slot[points] = (text, *options)

    boards = {}
    for label, themes in variants.items():
        if len(themes) != N_THEMES:
            errors.append(f"{source}: в варианте «{label}» {len(themes)} тем вместо {N_THEMES}: {', '.join(themes)}")
            continue
        bad = False
        for theme, slot in themes.items():
            gaps = [p for p in POINTS if p not in slot]
            if gaps:
                errors.append(f"{source}: в варианте «{label}», теме «{theme}» нет вопросов на {', '.join(map(str, gaps))}")
                bad = True
        if not bad:
            boards[label] = [
                {"name": theme, "questions": [_question(p, slot[p]) for p in POINTS]}
                for theme, slot in themes.items()
            ]
    if errors:
        raise QuestionsError("\n".join(errors))
    return boards


def load_variants(path=BASE_DIR / "questions.csv"):
    name = Path(path).name
    boards = parse_csv(read_text(path), name)
    odd = [k for k in boards if not k.isdigit() and k != "ML"]
    if odd:
        raise QuestionsError(f"{name}: «Вариант» — это номер (1, 2, …) или ML, а не {', '.join(odd)}")
    numbered = {int(k): b for k, b in boards.items() if k.isdigit()}
    if not numbered:
        raise QuestionsError(f"{name}: нет ни одного варианта")
    if sorted(numbered) != list(range(1, len(numbered) + 1)):
        raise QuestionsError(f"{name}: варианты должны идти подряд с 1, сейчас: {sorted(numbered)}")
    return [numbered[i] for i in sorted(numbered)], boards.get("ML")


def load_single_board(text, source):
    """The broadcast file holds exactly one board (its «Вариант» value doesn't matter)."""
    boards = parse_csv(text, source)
    if len(boards) != 1:
        raise QuestionsError(f"{source}: должен быть ровно один вариант, а найдено {len(boards)}: {', '.join(boards)}")
    return next(iter(boards.values()))


SETS, HARD = load_variants()
BOARD = SETS[0]
