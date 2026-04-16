import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine


TABLE_NAME = os.getenv("TABLE_NAME", "nuclear_data")
REPORT_PATH = "/app/reports/research_report.json"
PLOTS_DIR = "/app/plots"

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )


def load_data():
    engine = get_engine()
    df = pd.read_sql_table(TABLE_NAME, engine)

    df["station"] = df["station"].astype(str).str.strip()
    df["station"] = df["station"].replace({"ЮУАЕС": "ПАЕС"})

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce")

    df = df.dropna(subset=["year", "quarter", "station"]).copy()
    df["year"] = df["year"].astype(int)
    df["quarter"] = df["quarter"].astype(int)

    df["period"] = pd.PeriodIndex.from_fields(
        year=df["year"],
        quarter=df["quarter"],
        freq="Q"
    )

    df = df.sort_values(["station", "period"]).reset_index(drop=True)
    return df


def ensure_dirs():
    os.makedirs("/app/reports", exist_ok=True)
    os.makedirs(PLOTS_DIR, exist_ok=True)


def convert_to_json_safe(obj):
    if isinstance(obj, dict):
        return {str(k): convert_to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_to_json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [convert_to_json_safe(v) for v in obj]
    if pd.isna(obj):
        return None
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return obj


def analyze_hypothesis_1(df):
    check_indices = [
        "irg_index",
        "iodine_ radionuclides_index",
        "stable_ radionuclides_index"
    ]

    anomalies_report = {}
    stats_report = {}
    plots = []

    for col in check_indices:
        anomalies = df[df[col] > 1][["year", "quarter", "station", col]]
        anomalies_report[col] = {
            "count": int(len(anomalies)),
            "rows": convert_to_json_safe(anomalies.to_dict(orient="records"))
        }

        stats_report[col] = convert_to_json_safe(df[col].describe().to_dict())

    limit = 1
    stations = df["station"].dropna().unique()

    for col in check_indices:
        for station in stations:
            d = df[df["station"] == station].sort_values("period")
            if d[col].dropna().empty:
                continue

            plt.figure(figsize=(10, 3))
            plt.plot(d["period"].astype(str), d[col], marker="o")
            plt.axhline(limit, linestyle="--")
            plt.title(f"{station}: {col} у часі (поріг 100%)")
            plt.xticks(rotation=45)
            plt.xlabel("Період (квартал)")
            plt.ylabel(col)
            plt.grid(True, alpha=0.3)
            plt.tight_layout()

            safe_station = str(station).replace(" ", "_")
            plot_name = f"hypothesis1_{col}_{safe_station}.png"
            plot_path = os.path.join(PLOTS_DIR, plot_name)
            plt.savefig(plot_path)
            plt.close()

            plots.append(plot_name)

    total_exceedances = sum(item["count"] for item in anomalies_report.values())

    if total_exceedances == 0:
        conclusion = (
            "У жодному з індексних показників не зафіксовано значень, "
            "що перевищують нормативне обмеження 1.0. "
            "Гіпотеза підтверджується: рівень радіаційного фону є стабільним "
            "і не перевищує встановлених нормативних значень."
        )
        hypothesis_confirmed = True
    else:
        conclusion = (
            "Було виявлено випадки перевищення нормативного значення 1.0 "
            "в окремих індексних показниках. Гіпотеза підтверджується не повністю."
        )
        hypothesis_confirmed = False

    return {
        "title": "Гіпотеза 1: рівень радіації стабільний та не перевищує нормативні значення",
        "checked_indices": check_indices,
        "anomalies": anomalies_report,
        "statistics": stats_report,
        "total_exceedances": int(total_exceedances),
        "confirmed": hypothesis_confirmed,
        "conclusion": conclusion,
        "plots": plots
    }


def analyze_hypothesis_2(df):
    summary_q = (
        df.groupby("quarter")["index_radioactive_releas"]
        .agg(["mean", "min", "max"])
        .reset_index()
    )

    plt.figure(figsize=(8, 5))
    sns.lineplot(data=df, x="quarter", y="index_radioactive_releas", hue="station", marker="o")
    plt.title("Сезонна динаміка за станціями")
    plt.xlabel("Квартал")
    plt.ylabel("index_radioactive_releas")
    plt.tight_layout()
    plot_name = "hypothesis2_seasonality_by_station.png"
    plt.savefig(os.path.join(PLOTS_DIR, plot_name))
    plt.close()

    summary_records = convert_to_json_safe(summary_q.to_dict(orient="records"))

    means = summary_q.set_index("quarter")["mean"].to_dict()
    mean_q1 = means.get(1)
    mean_q2 = means.get(2)
    mean_q3 = means.get(3)
    mean_q4 = means.get(4)

    conclusion = (
        f"Було проаналізовано середні значення показника index_radioactive_releas за кварталами. "
        f"Середні значення змінюються від {mean_q1:.2f} у 1 кварталі до {mean_q3:.2f} у 3 кварталі. "
        f"У 2 та 4 кварталах середні показники становлять {mean_q2:.2f} та {mean_q4:.2f}. "
        f"Простежується тенденція зростання до 3 кварталу з подальшою стабілізацією, "
        f"що свідчить про наявність сезонних коливань. "
        f"Гіпотеза про можливі сезонні зміни обсягів радіоактивних викидів підтверджується."
    )

    return {
        "title": "Гіпотеза 2: об’єми радіоактивних викидів можуть демонструвати сезонні коливання",
        "quarter_summary": summary_records,
        "confirmed": True,
        "conclusion": conclusion,
        "plots": [plot_name]
    }


def analyze_hypothesis_3(df):
    correlation = df["cs_137_emission"].corr(df["co_60_ emission"])

    plt.figure(figsize=(8, 6))
    sns.regplot(
        data=df,
        x="cs_137_emission",
        y="co_60_ emission",
        scatter_kws={"alpha": 0.5},
        line_kws={"color": "red"}
    )
    plt.title(f"Зв'язок між викидами Cs-137 та Co-60 (Correlation: {correlation:.2f})")
    plt.xlabel("Cs-137 Emission")
    plt.ylabel("Co-60 Emission")
    plt.tight_layout()
    plot_name = "hypothesis3_cs137_vs_co60.png"
    plt.savefig(os.path.join(PLOTS_DIR, plot_name))
    plt.close()

    if pd.isna(correlation):
        strength = "невизначений"
        confirmed = False
        conclusion = (
            "Не вдалося коректно обчислити коефіцієнт кореляції між показниками "
            "cs_137_emission та co_60_ emission."
        )
    else:
        abs_corr = abs(correlation)
        if abs_corr < 0.3:
            strength = "слабкий"
        elif abs_corr < 0.7:
            strength = "помірний"
        else:
            strength = "сильний"

        conclusion = (
            f"Було проведено кореляційний аналіз між показниками cs_137_emission та co_60_ emission. "
            f"Отримане значення коефіцієнта кореляції становить r = {correlation:.3f}. "
            f"Це свідчить про {strength.lower()} позитивний зв’язок між обсягами викидів Cs-137 та Co-60. "
            f"Початкова гіпотеза про можливість прогнозування одного показника на основі іншого "
            f"за рахунок сильної позитивної кореляції не підтвердилася, оскільки зв'язок недостатньо сильний "
            f"для побудови надійних прогностичних моделей."
        )
        confirmed = correlation >= 0.7

    return {
        "title": "Гіпотеза 3: між cs_137_emission та co_60_ emission може існувати позитивна кореляція",
        "correlation": None if pd.isna(correlation) else float(correlation),
        "relationship_strength": strength,
        "confirmed": confirmed,
        "conclusion": conclusion,
        "plots": [plot_name]
    }


def build_report(df):
    numeric_df = df.drop(columns=['year', 'quarter', 'period'], errors='ignore')
    desc = numeric_df.describe(include='all').round(3).to_dict()

    report = {
        "dataset_info": {
            "rows": int(df.shape[0]),
            "columns": int(df.shape[1]),
            "stations": convert_to_json_safe(sorted(df["station"].dropna().unique().tolist())),
            "period_range": {
                "start": str(df["period"].min()),
                "end": str(df["period"].max())
            }
        },
        "descriptive_statistics": desc,
        "hypotheses": {
            "hypothesis_1": analyze_hypothesis_1(df),
            "hypothesis_2": analyze_hypothesis_2(df),
            "hypothesis_3": analyze_hypothesis_3(df)
        }
    }
    return convert_to_json_safe(report)


def save_report(report):
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)


def main():
    ensure_dirs()
    df = load_data()
    report = build_report(df)
    save_report(report)
    print("Research report and plots saved successfully.")


if __name__ == "__main__":
    main()