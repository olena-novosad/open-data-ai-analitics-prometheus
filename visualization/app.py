import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

TABLE_NAME = os.getenv("TABLE_NAME", "nuclear_data")
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

    df.columns = df.columns.str.strip().str.replace(" ", "_", regex=False)

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce")

    return df


def ensure_dirs():
    os.makedirs(PLOTS_DIR, exist_ok=True)


def save_histograms(df_plot, numeric_cols):
    for col in numeric_cols:
        plt.figure(figsize=(6, 4))
        sns.histplot(df_plot[col].dropna(), kde=True)
        plt.title(f"Distribution of {col}")
        plt.xlabel(col)
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, f"hist_{col}.png"))
        plt.close()


def save_boxplots(df_plot, numeric_cols):
    for col in numeric_cols:
        plt.figure(figsize=(6, 4))
        sns.boxplot(x=df_plot[col].dropna())
        plt.title(f"Boxplot of {col}")
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, f"boxplot_{col}.png"))
        plt.close()


def save_average_irg_by_station(df):
    if "station" in df.columns and "irg" in df.columns:
        avg_irg = df.groupby("station", as_index=False)["irg"].mean()

        plt.figure(figsize=(7, 5))
        sns.barplot(data=avg_irg, x="station", y="irg", hue="station", legend=False)
        plt.title("Average IRG by Station")
        plt.xlabel("Station")
        plt.ylabel("IRG")
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, "average_irg.png"))
        plt.close()


def save_irg_trend(df):
    if {"year", "quarter", "irg"}.issubset(df.columns):
        df_trend = df.dropna(subset=["year", "quarter"]).copy()
        df_trend["year"] = df_trend["year"].astype(int)
        df_trend["quarter"] = df_trend["quarter"].astype(int)
        df_trend["period"] = df_trend["year"].astype(str) + "-Q" + df_trend["quarter"].astype(str)

        period_mean = df_trend.groupby("period")["irg"].mean()

        plt.figure(figsize=(10, 5))
        period_mean.plot(marker="o")
        plt.title("Mean IRG by Quarter")
        plt.xlabel("Period")
        plt.ylabel("IRG")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, "irg_trend.png"))
        plt.close()


def save_scatter_irg_vs_cs137(df_plot):
    required_cols = {"irg", "cs_137_emission", "station"}
    if required_cols.issubset(df_plot.columns):
        plt.figure(figsize=(7, 5))
        sns.scatterplot(
            data=df_plot,
            x="irg",
            y="cs_137_emission",
            hue="station",
            alpha=0.7
        )
        plt.title("Relationship Between IRG and Cs-137 Emissions by NPP Station")
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, "irg_vs_cs137.png"))
        plt.close()


def main():
    ensure_dirs()
    df = load_data()

    df_plot = df.copy()

    cols_to_drop = ["year", "quarter", "iodine_radionuclides_index"]
    cols_to_drop = [c for c in cols_to_drop if c in df_plot.columns]
    df_plot = df_plot.drop(columns=cols_to_drop)

    numeric_cols = df_plot.select_dtypes(include=[np.number]).columns.tolist()

    save_histograms(df_plot, numeric_cols)
    save_boxplots(df_plot, numeric_cols)
    save_average_irg_by_station(df)
    save_irg_trend(df)
    save_scatter_irg_vs_cs137(df_plot)

    print("Visualization plots saved successfully.")


if __name__ == "__main__":
    main()