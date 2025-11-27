from pathlib import Path
import json
import pprint

BASE_DIR = Path(__file__).resolve().parent

INPUT_PATH = BASE_DIR/"_data"/"raw"/"employees_nested.json"

def main() -> None:
    print(f"Читаем файл:{INPUT_PATH}")

    with open(INPUT_PATH, 'r', encoding="utf-8") as f:
        data = json.load(f)

    print("Тип корневого объекта:", type(data))
    print("Количество элементов в списке:", len(data))

    print("\nПервый элемент (data[0]):")
    pprint.pp(data[0], sort_dicts=False)

if __name__ == "__main__":
    main()