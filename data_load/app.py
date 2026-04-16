import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import time

def wait_for_db():
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    db_name = os.getenv("POSTGRES_DB")

    while True:
        try:
            conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            conn.close()
            print("Database is ready")
            break
        except psycopg2.OperationalError:
            print("Waiting for database...")
            time.sleep(2)

def load_nuclear_data(file_path):
    if not os.path.exists(file_path):
        print(f"Помилка: файл {file_path} не знайдено.")
        return None

    df = pd.read_excel(file_path)

    df['station'] = df['station'].astype(str).str.strip()
    df['station'] = df['station'].replace({'ЮУАЕС': 'ПАЕС'})

    numeric_cols = df.columns.drop(['year', 'quarter', 'station'])

    for col in numeric_cols:
        df[col] = df[col].astype(str).str.replace(',', '.', regex=False).str.strip()
        df.loc[df[col].str.contains('<', na=False), col] = '0'
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def save_to_postgres(df):
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    db_name = os.getenv("POSTGRES_DB")

    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    df.to_sql("nuclear_data", engine, if_exists="replace", index=False)
    print("Дані успішно завантажені в PostgreSQL.")

def main():
    print("Waiting for DB...")
    wait_for_db()

    data_path = "/app/data/nuclear_safety.xlsx"

    df = load_nuclear_data(data_path)

    if df is not None:
        save_to_postgres(df)
        print(f"Успішно оброблено {df.shape[0]} рядків і {df.shape[1]} стовпців.")
    else:
        print("Не вдалося завантажити дані.")

if __name__ == "__main__":
    main()