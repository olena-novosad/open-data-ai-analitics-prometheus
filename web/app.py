import os
import json
from flask import Flask, render_template
import pandas as pd
from sqlalchemy import create_engine
from prometheus_flask_exporter import PrometheusMetrics # ДОДАНО: імпорт для метрик

app = Flask(__name__)

metrics = PrometheusMetrics(app)
metrics.info('app_info', 'Application info', version='1.0.0')

REPORTS_DIR = "/app/reports"
STATIC_PLOTS_DIR = "/app/static"
TABLE_NAME = os.getenv("TABLE_NAME", "nuclear_data")

PROJECT_INFO = {
    "title": "Open Data AI Analytics",
    "subtitle": "Аналіз екологічної та радіаційної обстановки в зоні розташування атомних електростанцій України",
    "goal": (
        "Метою проєкту є аналіз екологічної та радіаційної обстановки "
        "в районах розташування атомних електростанцій України на основі "
        "відкритих державних даних. Проєкт спрямований на виявлення "
        "закономірностей у розподілі радіонуклідів, а також на перевірку "
        "дотримання встановлених норм безпеки."
    ),
    "source_name": "Єдиний державний веб-портал відкритих даних",
    "source_url": "https://data.gov.ua/dataset/4a9d3d56-bd95-4c3e-97e7-1cdc7bcbd445",
    "source_description": (
        "Дані містять інформацію про показники радіаційного контролю "
        "в зоні розташування атомних електростанцій."
    )
}

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

def get_data_preview(limit=10):
    try:
        engine = get_engine()
        df = pd.read_sql_table(TABLE_NAME, engine).head(limit)
        columns = df.columns.tolist()
        rows = df.fillna("").to_dict(orient="records")
        return columns, rows
    except Exception:
        return [], []

def load_json_file(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def get_quality_report():
    return load_json_file(os.path.join(REPORTS_DIR, "quality_report.json")) or \
           load_json_file(os.path.join(REPORTS_DIR, "data_quality_report.json"))

def get_research_report():
    return load_json_file(os.path.join(REPORTS_DIR, "research_report.json"))

def get_plot_files():
    if not os.path.exists(STATIC_PLOTS_DIR):
        return []
    allowed_ext = {".png", ".jpg", ".jpeg", ".webp"}
    files = []
    for name in os.listdir(STATIC_PLOTS_DIR):
        _, ext = os.path.splitext(name.lower())
        if ext in allowed_ext:
            files.append(name)
    return sorted(files)

@app.route("/")
def index():
    quality_report = get_quality_report()
    research_report = get_research_report()
    plot_files = get_plot_files()
    columns, df_preview = get_data_preview()

    pretty_quality = json.dumps(quality_report, indent=2, ensure_ascii=False) if quality_report else None
    pretty_research = json.dumps(research_report, indent=2, ensure_ascii=False) if research_report else None

    return render_template(
        "index.html",
        project=PROJECT_INFO,
        quality_report=quality_report,
        research_report=research_report,
        plot_files=plot_files,
        columns=columns,
        df_preview=df_preview,
        pretty_quality=pretty_quality,
        pretty_research=pretty_research
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)