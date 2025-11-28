from pathlib import Path
import json
import pprint

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent

INPUT_PATH = BASE_DIR/"_data"/"raw"/"employees_nested.json"
OUTPUT_DIR = BASE_DIR/"_data"/"output"
OUTPUT_PATH = OUTPUT_DIR/"employees_tasks.csv"

def load_json() -> list:

    print(f"Читаем файл:{INPUT_PATH}")

    with open(INPUT_PATH, 'r', encoding="utf-8") as f:
        data = json.load(f)

    print("Тип корневого объекта:", type(data))
    print("Количество элементов в списке:", len(data))

    print("\nПервый элемент (data[0]):")
    pprint.pp(data[0], sort_dicts=False)

    return data

def flatten_tasks(data: list) -> pd.DataFrame:

    print("\nРазворачиваем JSON с помощью pandas.json_normalize ...")

    df = pd.json_normalize(
        data,
        record_path=["employee", "projects", "tasks"],
        meta=[
            ["employee", "id"],
            ["employee", "name"],
            ["employee", "position"],
            ["employee", "department", "id"],
            ["employee", "department", "name"],
            ["employee", "department", "manager", "id"],
            ["employee", "department", "manager", "name"],
            ["employee", "department", "manager", "contact", "email"],
            ["employee", "department", "manager", "contact", "phone"],
            ["employee", "projects", "projectId"],
            ["employee", "projects", "projectName"],
            ["employee", "projects", "startDate"],
        ],
    )

    print("Таблица собрана.")
    print("Размер таблицы (rows, cols):", df.shape)
    print("\nПервые строки:")
    print(df.head())

    return df

def save_to_csv(df: pd.DataFrame) -> None:

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"\nГотово! Файл сохранён в: {OUTPUT_PATH}")


def main() -> None:

    data = load_json()
    df = flatten_tasks(data)
    save_to_csv(df)

if __name__ == "__main__":
    main()