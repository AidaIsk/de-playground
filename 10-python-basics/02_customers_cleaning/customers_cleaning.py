import pandas as pd

INPUT_PATH = '_data/customers_raw.csv'
OUTPUT_PATH = '_data/output/customers_clean.csv'

def main():
    print('Загружаем данные ...')
    df = pd.read_csv(INPUT_PATH)

    if "Index" in df.columns:
        df = df.drop(columns=["Index"])

    text_cols = ["First Name", "Last Name", "Company", "City", "Country"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()
    
    df["City"] = df["City"].str.title()
    df["Country"] = df["Country"].str.title()

    df['Email'] = df['Email'].astype(str).str.strip().str.lower()
    df['Email Domain'] = df["Email"].str.split('@').str[-1]

    df["Subscription Date"] = pd.to_datetime(df["Subscription Date"], errors="coerce")


    print(f"Размер исходной таблицы: {df.shape}")
    print("Первые строки:")
    print(df.head())

    df.to_csv(OUTPUT_PATH, index = False)
    print(f"Данные сохранены в {OUTPUT_PATH}")

if __name__ == "__main__":
    main()





