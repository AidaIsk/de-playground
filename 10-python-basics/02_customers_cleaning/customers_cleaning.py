"""
Очистка учебного датасета с клиентами (customers_raw.csv).

Основные шаги программы:
1. Загружаем исходный CSV с клиентами.
2. Удаляем техническую колонку Index (если есть).
3. Чистим текстовые поля: убираем пробелы по краям.
4. Приводим названия городов и стран к аккуратному виду (City/ Country → Title Case).
5. Нормализуем Email:
   - убираем пробелы
   - приводим к нижнему регистру
   - выделяем домен в отдельную колонку Email Domain
6. Приводим Subscription Date к типу datetime (для дальнейшего анализа).
7. Сохраняем очищенный датасет в новый CSV-файл.

Это базовая задача по очистке данных в pandas: подготовка сырых данных к анализу.
"""

import pandas as pd

# Пути к входному и выходному файлам
INPUT_PATH = '_data/customers_raw.csv'
OUTPUT_PATH = '_data/output/customers_clean.csv'

def main():
    """Главная функция: загружаем, чистим и сохраняем данные."""
    print('Загружаем данные ...')
    df = pd.read_csv(INPUT_PATH)

    # 1. Удаляем колонку Index, если она есть в датасете
    if "Index" in df.columns:
        df = df.drop(columns=["Index"])

    # 2. Чистим пробелы в текстовых полях
    text_cols = ["First Name", "Last Name", "Company", "City", "Country"]
    for col in text_cols:
        # приводим к строкам и убираем пробелы по краям
        df[col] = df[col].astype(str).str.strip()
    
    # 3. Делаем города и страны в формате "Слово Слово"
    df["City"] = df["City"].str.title()
    df["Country"] = df["Country"].str.title()

    # 4. Нормализуем Email и выделяем домен
    df['Email'] = df['Email'].astype(str).str.strip().str.lower()
    df['Email Domain'] = df["Email"].str.split('@').str[-1]

    # 5. Приводим Subscription Date к типу datetime
    df["Subscription Date"] = pd.to_datetime(df["Subscription Date"], errors="coerce")

    # 6. Выводим краткую информацию о результате
    print(f"Размер исходной таблицы: {df.shape}")
    print("Первые строки:")
    print(df.head())

    # 7. Сохраняем результат в новый CSV-файл
    df.to_csv(OUTPUT_PATH, index = False)
    print(f"Данные сохранены в {OUTPUT_PATH}")

if __name__ == "__main__":
    main()





