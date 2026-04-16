import os
import json
import pandas as pd
from sqlalchemy import create_engine

TABLE_NAME = os.getenv("TABLE_NAME", "nuclear_data")


def get_db_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def load_data():
    engine = get_db_engine()
    df = pd.read_sql_table(TABLE_NAME, engine)
    return df


def analyze_data_quality(df):
    report = {}

    report["general_info"] = {
        "shape": {
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1])
        },
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
    }

    missing_counts = df.isna().sum()
    missing_ratio = (df.isna().mean()).sort_values(ascending=False)

    report["missing_values"] = {
        "total_missing_per_column": missing_counts.to_dict(),
        "missing_ratio_per_column": missing_ratio.to_dict()
    }

    duplicate_count = int(df.duplicated().sum())
    report["duplicates"] = {
        "duplicate_rows_count": duplicate_count
    }

    stations = df["station"].dropna().unique()
    expected_stations = {"ЗАЕС", "РАЕС", "ПАЕС", "ХАЕС"}

    unexpected_stations = sorted(set(stations) - expected_stations)

    report["stations"] = {
        "unique_count": int(len(stations)),
        "list": stations.tolist(),
        "unexpected_stations": unexpected_stations
    }

    rows_with_nan = df[df.isnull().any(axis=1)]
    report["rows_with_missing"] = {
        "count": int(len(rows_with_nan))
    }

    expected_types = {
        "year": "int64",
        "quarter": "int64",
        "station": "str"
    }

    type_issues = {}
    for col, expected_type in expected_types.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if actual_type != expected_type:
                type_issues[col] = {
                    "expected": expected_type,
                    "actual": actual_type
                }

    report["type_validation"] = {
        "valid": len(type_issues) == 0,
        "issues": type_issues
    }

    value_issues = {}

    if "quarter" in df.columns:
        invalid_quarter = df[~df["quarter"].isin([1, 2, 3, 4])]
        value_issues["quarter_out_of_range"] = {
            "count": int(len(invalid_quarter))
        }

    if "year" in df.columns:
        invalid_year = df[df["year"].isna()]
        value_issues["invalid_year"] = {
            "count": int(len(invalid_year))
        }

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    negative_values = {}

    for col in numeric_cols:
        negative_count = int((df[col] < 0).sum())
        if negative_count > 0:
            negative_values[col] = negative_count

    value_issues["negative_values"] = negative_values

    report["value_validation"] = {
        "valid": (
            value_issues["quarter_out_of_range"]["count"] == 0
            and value_issues["invalid_year"]["count"] == 0
            and len(negative_values) == 0
            and len(unexpected_stations) == 0
        ),
        "issues": value_issues
    }

    conclusions = []

    if missing_counts.sum() > 0:
        conclusions.append(
            "У наборі даних виявлено пропущені значення, які слід враховувати під час подальшого аналізу."
        )
    else:
        conclusions.append(
            "Пропущених значень у наборі даних не виявлено."
        )

    if duplicate_count > 0:
        conclusions.append(
            f"Виявлено {duplicate_count} дублікатів рядків, що може впливати на результати аналізу."
        )
    else:
        conclusions.append(
            "Дублікатів рядків не виявлено."
        )

    if len(type_issues) > 0:
        conclusions.append(
            "Виявлено невідповідності типів даних у деяких стовпцях."
        )
    else:
        conclusions.append(
            "Типи даних у ключових стовпцях відповідають очікуваним."
        )

    if report["value_validation"]["valid"]:
        conclusions.append(
            "Некоректних значень у ключових полях не виявлено."
        )
    else:
        conclusions.append(
            "Виявлено окремі некоректні значення, які потребують уваги перед поглибленим аналізом."
        )

    report["analysis_conclusions"] = {
        "missing_data_analysis": (
            "Аналіз пропущених значень виявив два ключові аспекти. "
            "По-перше, з 2022 року повністю відсутні дані щодо Запорізької АЕС, "
            "що пов’язано з воєнними діями. По-друге, для Хмельницької АЕС "
            "відсутні дані за показником co_60_dump (скиди Кобальту-60). "
            "Це може свідчити про специфіку моніторингу або про незафіксовані нульові значення."
        ),
        "data_handling_recommendations": (
            "Ці особливості необхідно враховувати під час подальшого аналізу та візуалізації результатів. "
            "Видалення таких записів є недоцільним через невеликий обсяг даних. "
            "Заповнення пропусків середніми значеннями також не є коректним для даних екологічного "
            "та радіологічного характеру, оскільки це може спотворити реальну картину показників."
        ),
        "quality_summary": conclusions
    }

    return report


def save_report(report):
    os.makedirs("/app/reports", exist_ok=True)

    with open("/app/reports/data_quality_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)

    print("Data quality report saved")


def main():
    df = load_data()
    report = analyze_data_quality(df)
    save_report(report)


if __name__ == "__main__":
    main()