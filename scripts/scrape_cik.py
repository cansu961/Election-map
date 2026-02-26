"""
Скрейпер данных о выборах Президента РФ с официального сайта ЦИК РФ
Целевой сайт: https://www.vybory.izbirkom.ru/

Собирает: per-candidate × per-region percentages + явка
Выборы: 1991, 1996 (два тура), 2000, 2024

Требования:
    pip install requests beautifulsoup4 lxml

Запуск (из корня проекта или из папки scripts/):
    python scripts/scrape_cik.py
    python scripts/scrape_cik.py --years 2024
    python scripts/scrape_cik.py --years 2024 2000 1996

Результат:
    Portal/data/president_regions.json  — обновлённый (все годы)
    Portal/data/scraped/YEAR.json       — отдельный файл на каждый год
"""

import os, sys, re, json, time, argparse
import requests
from bs4 import BeautifulSoup

# ─── Пути ────────────────────────────────────────────────────────────────────
_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PORTAL_DIR  = os.path.dirname(_SCRIPT_DIR)          # Portal/
REGIONS_CSV  = os.path.join(_PORTAL_DIR, 'Vybory', 'vybory_regions_key.csv')
OUT_DIR      = os.path.join(_PORTAL_DIR, 'data')
SCRAPED_DIR  = os.path.join(OUT_DIR, 'scraped')
PRES_JSON    = os.path.join(OUT_DIR, 'president_regions.json')

# ─── HTTP-сессия ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/121.0.0.0 Safari/537.36',
    'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection':      'keep-alive',
    'Referer':         'https://www.vybory.izbirkom.ru/',
})
DELAY = 1.5   # секунд между запросами

# ─── URL для каждых выборов Президента ───────────────────────────────────────
# type=226  → результаты по субъектам РФ (кандидаты × регионы)
_BASE = ('https://www.vybory.izbirkom.ru/region/region/izbirkom'
         '?action=show&root_a=412'
         '&tvd={tvd}&vrn={vrn}'
         '&region=0&global=1&sub_region=0&prver=0&pronetvd=null'
         '&vibid={vrn}&type=226')

ELECTIONS = {
    '2024': dict(
        tvd='100100084849066', vrn='100100084849062',
        year=2024, date='15–17 марта 2024',
        title='Выборы Президента РФ 2024',
        id='president_2024',
    ),
    '2018': dict(
        tvd='100100084849065', vrn='100100084849061',
        year=2018, date='18 марта 2018',
        title='Выборы Президента РФ 2018',
        id='president_2018',
    ),
    '2012': dict(
        tvd='100100022336596', vrn='100100022336812',
        year=2012, date='4 марта 2012',
        title='Выборы Президента РФ 2012',
        id='president_2012',
    ),
    '2008': dict(
        tvd='100100021960070', vrn='100100021960066',
        year=2008, date='2 марта 2008',
        title='Выборы Президента РФ 2008',
        id='president_2008',
    ),
    '2004': dict(
        tvd='100100021596090', vrn='100100021596451',
        year=2004, date='14 марта 2004',
        title='Выборы Президента РФ 2004',
        id='president_2004',
    ),
    '2000': dict(
        tvd='100100020800339', vrn='100100020800085',
        year=2000, date='26 марта 2000',
        title='Выборы Президента РФ 2000',
        id='president_2000',
    ),
    '1996r1': dict(
        tvd='100100020578856', vrn='100100020578765',
        year=1996, date='16 июня 1996',
        title='Выборы Президента РФ 1996 (1 тур)',
        id='president_1996_r1',
    ),
    '1996r2': dict(
        tvd='100100020578857', vrn='100100020578766',
        year=1996, date='3 июля 1996',
        title='Выборы Президента РФ 1996 (2 тур)',
        id='president_1996_r2',
    ),
    '1991': dict(
        tvd='100100020404560', vrn='100100020404500',
        year=1991, date='12 июня 1991',
        title='Выборы Президента РСФСР 1991',
        id='president_1991',
    ),
}

# Цвета кандидатов по фамилии
CAND_COLORS = {
    'путин':         '#1565C0',
    'харитонов':     '#e53935',
    'даванков':      '#4CAF50',
    'слуцкий':       '#FF9800',
    'грудинин':      '#e53935',
    'жириновский':   '#FF9800',
    'собчак':        '#E91E63',
    'сурайкин':      '#9E9E9E',
    'бабурин':       '#78909C',
    'титов':         '#8BC34A',
    'явлинский':     '#4CAF50',
    'зюганов':       '#b71c1c',
    'медведев':      '#1565C0',
    'богданов':      '#607D8B',
    'прохоров':      '#607D8B',
    'миронов':       '#4CAF50',
    'глазьев':       '#FF5722',
    'хакамада':      '#E91E63',
    'малышкин':      '#FF9800',
    'рыжков':        '#e53935',
    'лебедь':        '#607D8B',
    'тулеев':        '#795548',
    'макашов':       '#607D8B',
    'бакатин':       '#9C27B0',
    'ельцин':        '#1565C0',
    'горбачёв':      '#9E9E9E',
    'шаккум':        '#9E9E9E',
    'власов':        '#9E9E9E',
    'брынцалов':     '#9E9E9E',
    'памфилова':     '#E91E63',
    'говорухин':     '#9C27B0',
    'скуратов':      '#9E9E9E',
    'подберёзкин':   '#9E9E9E',
    'джабраилов':    '#9E9E9E',
    'против всех':   '#9E9E9E',
}

def get_color(name_raw):
    nl = name_raw.lower()
    for key, color in CAND_COLORS.items():
        if key in nl:
            return color
    return '#9E9E9E'


# ─── Загрузка словаря регионов ───────────────────────────────────────────────
REG_NAME_TO_KEY = {}
_NORM_MAP       = {}

MANUAL_REGIONS = {
    'г. москва':                               'moskva',
    'москва':                                  'moskva',
    'г. санкт-петербург':                      'spb',
    'санкт-петербург':                         'spb',
    'ленинград':                               'spb',
    'ямало-ненецкий автономный округ':         'yamalo_nenetskiy',
    'ненецкий автономный округ':               'nenetskiy',
    'ханты-мансийский автономный округ - югра':'hmao',
    'ханты-мансийский автономный округ — югра':'hmao',
    'ханты-мансийский автономный округ':       'hmao',
    'чукотский автономный округ':              'chukotskiy',
    'еврейская автономная область':            'evreyskaya',
    'республика северная осетия - алания':     'severnaya_osetiya',
    'республика северная осетия':              'severnaya_osetiya',
    'кемеровская область - кузбасс':           'kemerovskaya',
    'кемеровская область':                     'kemerovskaya',
    # Исторические (2000/1996/1991)
    'камчатская область':                      'kamchatskiy',
    'пермская область':                        'permskiy',
    'читинская область':                       'zabaykalskiy',
    'чечено-ингушетия':                        'chechenskaya',  # 1991
    'чечено-ингушская республика':             'chechenskaya',
    'ингушская республика':                    'ingushetiya',
}

# Регионы которые пропускаем
SKIP_REGIONS = {
    'российская федерация', 'россия', 'сумма',
    'город байконур', 'байконур',
    'территория за пределами рф',
    'территории за рубежом',
    'за рубежом',
    'агинский бурятский автономный округ',
    'коми-пермяцкий автономный округ',
    'корякский автономный округ',
    'таймырский (долгано-ненецкий) автономный округ',
    'усть-ордынский бурятский автономный округ',
    'эвенкийский автономный округ',
}

def load_regions():
    if not os.path.exists(REGIONS_CSV):
        print(f'  ПРЕДУПРЕЖДЕНИЕ: {REGIONS_CSV} не найден, используем только MANUAL_REGIONS')
        return
    with open(REGIONS_CSV, encoding='utf-8') as f:
        lines = f.read().splitlines()
    for line in lines[1:]:
        parts = line.split(',', 4)
        if len(parts) >= 2:
            key, name = parts[0].strip(), parts[1].strip()
            REG_NAME_TO_KEY[name] = key

def _normalize(s):
    s = str(s).strip().lower()
    s = re.sub(r'\s*\([^)]*\)\s*', ' ', s).strip()
    s = re.sub(r'\s*[—–-]\s*\w+\s*$', '', s).strip()
    return s

def build_norm_map():
    for name, key in REG_NAME_TO_KEY.items():
        _NORM_MAP[_normalize(name)] = key

def find_key(name_raw):
    if not name_raw:
        return None
    s = str(name_raw).strip()
    sl = s.lower()
    # Пропустить?
    if sl in SKIP_REGIONS or any(sk in sl for sk in SKIP_REGIONS):
        return '__SKIP__'
    # Ручная таблица
    if sl in MANUAL_REGIONS:
        return MANUAL_REGIONS[sl]
    # Точное совпадение
    if s in REG_NAME_TO_KEY:
        return REG_NAME_TO_KEY[s]
    # Нормализованное
    n = _normalize(s)
    if n in _NORM_MAP:
        return _NORM_MAP[n]
    # Частичное
    for nk, rk in _NORM_MAP.items():
        if len(nk) > 5 and (nk in n or n in nk):
            return rk
    return None


# ─── HTTP-запрос ─────────────────────────────────────────────────────────────
def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, timeout=20)
            resp.raise_for_status()
            # ЦИК может отдавать windows-1251
            if resp.encoding and resp.encoding.lower() in ('windows-1251', 'cp1251', 'iso-8859-5'):
                text = resp.content.decode('windows-1251', errors='replace')
            else:
                text = resp.text
            return text
        except requests.RequestException as e:
            print(f'    Попытка {attempt+1}/{retries} не удалась: {e}')
            if attempt < retries - 1:
                time.sleep(DELAY * 2)
    return None


# ─── Парсинг страницы ЦИК ────────────────────────────────────────────────────
def parse_cik_page(html):
    """
    Парсит страницу результатов ЦИК типа 226 (по субъектам РФ).
    Возвращает:
        candidates: list of {raw_name, pcts: {region_key: float}, pct_national: float}
        turnout_map: {region_key: float}
        regions_order: list of (col_idx, key, name)
    """
    soup = BeautifulSoup(html, 'lxml')

    # Ищем основную таблицу с результатами
    # ЦИК использует разные классы в разные годы — пробуем несколько вариантов
    table = (
        soup.find('table', {'class': re.compile(r'(sdelect|election|result)', re.I)})
        or soup.find('table', id=re.compile(r'(result|table)', re.I))
        or _find_main_table(soup)
    )

    if not table:
        print('    ОШИБКА: таблица с результатами не найдена')
        return None, None, None

    rows = table.find_all('tr')
    if len(rows) < 3:
        print(f'    ОШИБКА: таблица слишком мала ({len(rows)} строк)')
        return None, None, None

    # Строка 1 — заголовки (регионы расположены по СТОЛБЦАМ)
    # Структура ЦИК type=226: строки = кандидаты, столбцы = регионы
    # Или наоборот: строки = регионы, столбцы = кандидаты
    # Определяем ориентацию по содержимому первой строки

    header_cells = rows[0].find_all(['td', 'th'])
    header_texts = [c.get_text(strip=True) for c in header_cells]

    print(f'    Заголовки (первые 5): {header_texts[:5]}')
    print(f'    Строк в таблице: {len(rows)}')

    # ЦИК type=226 обычно: первая строка = названия регионов (по горизонтали)
    # Каждая следующая строка — один кандидат
    # Первая ячейка строки кандидата — его имя

    # Собираем колонки регионов из первой строки
    regions_order = []  # (col_idx, key, name)
    nat_col = None      # индекс столбца с суммарным (Россия в целом)

    for ci, text in enumerate(header_texts):
        if not text:
            continue
        tl = text.strip().lower()
        if tl in SKIP_REGIONS or 'российская федерация' in tl or 'россия' in tl or tl == 'россия':
            nat_col = ci
            continue
        key = find_key(text.strip())
        if key == '__SKIP__' or key is None:
            if key is None:
                print(f'    НЕРАСПОЗНАН: "{text.strip()}"')
        else:
            regions_order.append((ci, key, text.strip()))

    print(f'    Регионов распознано: {len(regions_order)}, nat_col={nat_col}')

    # Данные кандидатов (строки 1..)
    candidates = []
    turnout_map = {}

    for row in rows[1:]:
        cells = row.find_all(['td', 'th'])
        texts = [c.get_text(strip=True) for c in cells]
        if not texts or not texts[0]:
            continue

        name_raw = texts[0].strip()
        name_lower = name_raw.lower()

        # Явка
        is_turnout = any(w in name_lower for w in ('явка', 'turnout', 'явка избирателей'))
        # Пропускаем строки с административными данными
        is_admin = any(w in name_lower for w in (
            'число', 'бюллетен', 'не учтен', 'список', 'зарегистр',
            'получен', 'погашен', 'выдан', 'недействитель', 'действительн',
            'досрочн', 'помещен', 'избиратель', 'итого',
        ))

        if is_admin and not is_turnout:
            continue

        def parse_pct(v):
            """Парсим значение — может быть "12,34" или "12.34" или "1234567" (абс. число)."""
            if not v:
                return None
            v = str(v).replace(',', '.').replace('\xa0', '').replace(' ', '')
            try:
                f = float(v)
                # Если значение > 100 — вероятно абсолютное число (голоса), не %
                return None if f > 100 else round(f, 2)
            except ValueError:
                return None

        if is_turnout:
            # Явка
            for ci, key, rname in regions_order:
                if ci < len(texts):
                    v = parse_pct(texts[ci])
                    if v is not None:
                        turnout_map[key] = v
        else:
            # Кандидат
            pcts = {}
            for ci, key, rname in regions_order:
                if ci < len(texts):
                    v = parse_pct(texts[ci])
                    if v is not None:
                        pcts[key] = v
            if pcts:
                nat_pct = None
                if nat_col is not None and nat_col < len(texts):
                    nat_pct = parse_pct(texts[nat_col])
                candidates.append({
                    'raw_name':     name_raw,
                    'pcts':         pcts,
                    'pct_national': nat_pct,
                })

    return regions_order, candidates, turnout_map


def _find_main_table(soup):
    """Эвристический поиск основной таблицы с результатами."""
    tables = soup.find_all('table')
    best = None
    best_cols = 0
    for t in tables:
        rows = t.find_all('tr')
        if not rows:
            continue
        cols = len(rows[0].find_all(['td', 'th']))
        if cols > best_cols:
            best_cols = cols
            best = t
    return best if best_cols > 5 else None


# ─── Формирование структуры кандидата ────────────────────────────────────────
def build_candidate(raw_name, pcts, pct_national):
    """Из сырого имени строим полную структуру кандидата."""
    name = raw_name.strip()
    # Пытаемся сократить до Фамилия И.О.
    parts = name.split()
    if len(parts) >= 3:
        short = parts[0] + ' ' + parts[1][0] + '.' + parts[2][0] + '.'
    elif len(parts) == 2:
        short = parts[0] + ' ' + parts[1][0] + '.'
    else:
        short = name

    return {
        'name':         short,
        'raw_name':     name,
        'party':        '',
        'color':        get_color(name),
        'pct_national': pct_national,
        'regions':      pcts,
    }


# ─── Основная функция скрейпинга одних выборов ───────────────────────────────
def scrape_election(key, meta):
    url = _BASE.format(tvd=meta['tvd'], vrn=meta['vrn'])
    print(f'\n  Загружаем: {meta["title"]}')
    print(f'  URL: {url}')

    time.sleep(DELAY)
    html = fetch(url)
    if not html:
        print(f'  ОШИБКА: не удалось загрузить страницу')
        return None

    # Быстрая проверка — страница похожа на результаты выборов?
    if 'избирател' not in html.lower() and 'кандидат' not in html.lower():
        print(f'  ПРЕДУПРЕЖДЕНИЕ: страница не похожа на результаты выборов')
        # Сохраняем для диагностики
        _save_debug_html(key, html)
        return None

    regions_order, candidates, turnout_map = parse_cik_page(html)
    if candidates is None:
        return None

    print(f'  Найдено кандидатов: {len(candidates)}')
    for c in candidates:
        print(f'    {c["raw_name"][:40]:40s} нац={c["pct_national"]}%  рег={len(c["pcts"])}')

    result = {
        'id':         meta.get('id', f'president_{meta["year"]}'),
        'year':       meta['year'],
        'date':       meta['date'],
        'title':      meta['title'],
        'source':     'cikrf.ru',
        'candidates': [
            build_candidate(c['raw_name'], c['pcts'], c['pct_national'])
            for c in candidates
        ],
        'turnout': turnout_map,
    }
    return result


def _save_debug_html(key, html):
    debug_dir = os.path.join(SCRAPED_DIR, 'debug')
    os.makedirs(debug_dir, exist_ok=True)
    path = os.path.join(debug_dir, f'{key}.html')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f'  Сохранён отладочный HTML: {path}')


# ─── Слияние с существующим president_regions.json ───────────────────────────
def merge_into_main(scraped_results):
    """
    Обновляет Portal/data/president_regions.json:
    - Для каждого scraped_result заменяет запись с тем же year/id
    - Записи без скрейпинга остаются нетронутыми
    """
    existing = []
    if os.path.exists(PRES_JSON):
        with open(PRES_JSON, encoding='utf-8') as f:
            existing = json.load(f)

    # Индексируем по id
    idx = {rec.get('id', str(rec.get('year'))): rec for rec in existing}

    updated = 0
    for rec in scraped_results:
        rid = rec.get('id', str(rec.get('year')))
        if rid in idx:
            # Обновляем только кандидатов и явку (не трогаем meta)
            idx[rid]['candidates'] = rec['candidates']
            idx[rid]['turnout']    = rec['turnout']
            idx[rid]['source']     = rec.get('source', 'cikrf.ru')
            print(f'  Обновлён: {rid}')
        else:
            idx[rid] = rec
            print(f'  Добавлен: {rid}')
        updated += 1

    out_list = sorted(idx.values(), key=lambda r: (r.get('year', 0), r.get('id', '')))

    with open(PRES_JSON, 'w', encoding='utf-8') as f:
        json.dump(out_list, f, ensure_ascii=False, indent=2)
    size = os.path.getsize(PRES_JSON)
    print(f'\n  president_regions.json обновлён: {len(out_list)} записей, {size//1024} KB')
    return updated


# ─── CLI ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='Скрейпер результатов выборов Президента РФ с сайта ЦИК')
    parser.add_argument(
        '--years', nargs='+', default=['2024', '2000', '1996r1', '1996r2', '1991'],
        help='Ключи выборов для скрейпинга (default: 2024 2000 1996r1 1996r2 1991)')
    parser.add_argument(
        '--all', action='store_true',
        help='Скрейпить все годы (включая 2004-2018)')
    parser.add_argument(
        '--test', action='store_true',
        help='Только проверить доступность ЦИК, не скачивать')
    args = parser.parse_args()

    print('=' * 60)
    print('  Скрейпер ЦИК РФ — выборы Президента')
    print('=' * 60)

    # Загружаем словарь регионов
    load_regions()
    build_norm_map()
    print(f'Словарь регионов: {len(REG_NAME_TO_KEY)} записей')

    # Тест подключения
    print('\nПроверка подключения к ЦИК...')
    test_url = 'https://www.vybory.izbirkom.ru/'
    try:
        resp = SESSION.get(test_url, timeout=10)
        print(f'  OK: {resp.status_code}')
    except Exception as e:
        print(f'  ОШИБКА подключения: {e}')
        print('  Проверьте интернет-соединение и доступность vybory.izbirkom.ru')
        sys.exit(1)

    if args.test:
        print('\nРежим --test: подключение успешно, выход.')
        return

    # Создаём папки
    os.makedirs(SCRAPED_DIR, exist_ok=True)

    # Определяем какие выборы скрейпить
    if args.all:
        targets = list(ELECTIONS.keys())
    else:
        targets = args.years

    invalid = [y for y in targets if y not in ELECTIONS]
    if invalid:
        print(f'Неизвестные ключи: {invalid}')
        print(f'Доступные: {list(ELECTIONS.keys())}')
        sys.exit(1)

    print(f'\nЗапланировано: {targets}')

    # Скрейпим
    scraped_results = []
    errors = []

    for key in targets:
        meta = ELECTIONS[key]
        result = scrape_election(key, meta)
        if result:
            # Сохраняем отдельный файл
            out_file = os.path.join(SCRAPED_DIR, f'{key}.json')
            with open(out_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f'  Сохранён: {out_file}')
            scraped_results.append(result)
        else:
            errors.append(key)

    # Итог
    print(f'\n{"="*60}')
    print(f'Успешно: {len(scraped_results)}/{len(targets)} выборов')
    if errors:
        print(f'Ошибки:  {errors}')

    if scraped_results:
        print('\nОбновляем president_regions.json...')
        merge_into_main(scraped_results)

    print('\nГотово!')


if __name__ == '__main__':
    main()
