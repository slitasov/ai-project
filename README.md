## Текущая структура проекта

```
ai_project/
├── data/                 # Каталог для данных (создаётся коллектором)
│   └── bybit-linear/
│       └── YYYY-MM-DD/
│           └── {symbol}/   # quotes.gz, trades.gz
├── md_collector.py        # Сбор данных Bybit (quotes + trades)
├── requirements.txt
└── README.md
```

## Установка

На macOS (Homebrew Python) пакеты нужно ставить в виртуальное окружение, иначе будет ошибка `externally-managed-environment`:

```bash
python3 -m venv .venv
source .venv/bin/activate   # на Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Дальше запускайте скрипты с активированным venv (`source .venv/bin/activate` в этой папке). Если включён ещё и conda (base), используйте явно интерпретатор venv: `python3 md_collector.py ...` или `.venv/bin/python md_collector.py ...`.

## Сбор данных

Скрипт `md_collector.py` собирает данные Bybit Linear (orderbook.1 и publicTrade) и сохраняет их в gzip-CSV.

**Запуск** (результат в `{output_dir}/bybit-linear/YYYY-MM-DD/{symbol}/`, символ в нижнем регистре, напр. `ethusdt`):
```bash
python3 md_collector.py --output-dir ./data BTCUSDT ETHUSDT
```

Файлы: `quotes.gz`, `trades.gz` в каждой папке по символу и дате.
