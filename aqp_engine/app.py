import os
import time
import random
import re
import numpy as np
import pandas as pd
import duckdb
import streamlit as st
import plotly.graph_objects as go

from config import DATA_PATH, DEFAULT_ROWS, DEFAULT_SAMPLE_FRACTION, RANDOM_SEED


# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="Approximate Query Engine",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================
# PREMIUM STYLING
# =========================
st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at 10% 10%, rgba(59,130,246,0.18), transparent 22%),
            radial-gradient(circle at 90% 10%, rgba(236,72,153,0.14), transparent 22%),
            radial-gradient(circle at 10% 90%, rgba(16,185,129,0.10), transparent 20%),
            linear-gradient(180deg, #030712 0%, #0b1120 55%, #0f172a 100%);
        color: #f8fafc;
    }

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(2,6,23,0.98), rgba(15,23,42,0.96));
        border-right: 1px solid rgba(255,255,255,0.06);
    }

    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2.4rem;
        max-width: 1420px;
    }

    .hero {
        position: relative;
        overflow: hidden;
        background:
            linear-gradient(135deg, rgba(15,23,42,0.92), rgba(30,41,59,0.82)),
            radial-gradient(circle at top right, rgba(99,102,241,0.35), transparent 32%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 28px;
        padding: 1.65rem 1.65rem 1.35rem 1.65rem;
        box-shadow: 0 30px 70px rgba(0,0,0,0.30);
        margin-bottom: 1rem;
    }

    .hero::after {
        content: "";
        position: absolute;
        inset: 0;
        background: radial-gradient(circle at 85% 15%, rgba(236,72,153,0.18), transparent 24%);
        pointer-events: none;
    }

    .hero-title {
        font-size: 2.9rem;
        font-weight: 900;
        color: white;
        letter-spacing: -0.04em;
        margin-bottom: 0.3rem;
        line-height: 1.05;
    }

    .hero-subtitle {
        font-size: 1.02rem;
        line-height: 1.65;
        color: #cbd5e1;
        max-width: 980px;
        margin-bottom: 1rem;
    }

    .badge {
        display: inline-block;
        margin-right: 0.45rem;
        margin-bottom: 0.45rem;
        padding: 0.35rem 0.78rem;
        border-radius: 999px;
        font-size: 0.82rem;
        font-weight: 700;
        color: white;
        background: linear-gradient(135deg, rgba(37,99,235,0.92), rgba(124,58,237,0.92));
        border: 1px solid rgba(255,255,255,0.10);
        box-shadow: 0 6px 16px rgba(59,130,246,0.18);
    }

    .section-card {
        background: linear-gradient(180deg, rgba(15,23,42,0.90), rgba(17,24,39,0.88));
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 24px;
        padding: 1.1rem 1.1rem 1.15rem 1.1rem;
        box-shadow: 0 18px 44px rgba(0,0,0,0.24);
        margin-bottom: 1.2rem;
    }

    .section-title {
        font-size: 1.28rem;
        font-weight: 800;
        color: #ffffff;
        margin-bottom: 0.25rem;
        line-height: 1.2;
    }

    .section-caption {
        color: #94a3b8;
        font-size: 0.93rem;
        margin-bottom: 0.85rem;
    }

    .glass-card {
        background: linear-gradient(180deg, rgba(15,23,42,0.96), rgba(30,41,59,0.86));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 1rem;
        box-shadow: 0 14px 36px rgba(0,0,0,0.22);
        margin-bottom: 1rem;
        height: 100%;
    }

    .kpi-card {
        background:
            linear-gradient(180deg, rgba(15,23,42,0.96), rgba(30,41,59,0.90));
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 0.95rem 1rem 0.9rem 1rem;
        box-shadow: 0 12px 36px rgba(0,0,0,0.24);
        position: relative;
        overflow: hidden;
        min-height: 118px;
    }

    .kpi-card::before {
        content: "";
        position: absolute;
        top: -35px;
        right: -35px;
        width: 110px;
        height: 110px;
        background: radial-gradient(circle, rgba(59,130,246,0.22), transparent 65%);
    }

    .kpi-label {
        color: #94a3b8;
        font-size: 0.84rem;
        margin-bottom: 0.2rem;
        position: relative;
        z-index: 1;
    }

    .kpi-value {
        color: #ffffff;
        font-size: 1.45rem;
        font-weight: 900;
        position: relative;
        z-index: 1;
        line-height: 1.15;
        word-break: break-word;
    }

    .kpi-sub {
        color: #cbd5e1;
        font-size: 0.83rem;
        margin-top: 0.38rem;
        position: relative;
        z-index: 1;
        line-height: 1.35;
    }

    .kpi-positive::before {
        background: radial-gradient(circle, rgba(16,185,129,0.24), transparent 65%);
    }

    .kpi-pink::before {
        background: radial-gradient(circle, rgba(236,72,153,0.22), transparent 65%);
    }

    .kpi-gold::before {
        background: radial-gradient(circle, rgba(250,204,21,0.20), transparent 65%);
    }

    div[data-testid="stMetric"] {
        background:
            linear-gradient(180deg, rgba(15,23,42,0.96), rgba(30,41,59,0.90));
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 18px;
        padding: 0.95rem 1rem;
        box-shadow: 0 12px 28px rgba(0,0,0,0.22);
    }

    div[data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }

    div[data-testid="stMetricValue"] {
        color: #ffffff !important;
    }

    .stButton > button, div[data-testid="stDownloadButton"] > button {
        width: 100%;
        border-radius: 15px;
        border: 1px solid rgba(255,255,255,0.09);
        background: linear-gradient(135deg, #2563eb, #7c3aed);
        color: white;
        font-weight: 800;
        padding: 0.66rem 1rem;
        box-shadow: 0 12px 28px rgba(59,130,246,0.22);
        margin-top: 0.15rem;
    }

    .stButton > button:hover, div[data-testid="stDownloadButton"] > button:hover {
        background: linear-gradient(135deg, #3b82f6, #8b5cf6);
        color: white;
        border: 1px solid rgba(255,255,255,0.15);
    }

    div[data-baseweb="select"] > div,
    div[data-baseweb="input"] > div,
    textarea {
        border-radius: 14px !important;
        background-color: rgba(15,23,42,0.92) !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
    }

    .stSlider [data-baseweb="slider"] {
        padding-top: 0.4rem;
        padding-bottom: 0.1rem;
    }

    div[data-testid="stDataFrame"] {
        margin-top: 1rem;
        padding-top: 0.2rem;
    }

    [data-testid="column"] {
        padding-top: 0.3rem;
    }

    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.14), transparent);
        margin-top: 0.55rem;
        margin-bottom: 0.8rem;
    }

    .tiny-gap {
        height: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================
# DATA GENERATION
# =========================
def generate_dataset(csv_path: str, n_rows: int = DEFAULT_ROWS) -> None:
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

    countries = ["India", "USA", "UK", "Germany", "Canada"]
    devices = ["Mobile", "Desktop", "Tablet"]
    campaigns = ["Summer", "Festive", "Launch", "Retargeting", "Referral"]

    df = pd.DataFrame(
        {
            "user_id": np.random.randint(10000, 99999, size=n_rows),
            "country": np.random.choice(
                countries, size=n_rows, p=[0.4, 0.2, 0.15, 0.15, 0.1]
            ),
            "device": np.random.choice(devices, size=n_rows, p=[0.6, 0.3, 0.1]),
            "campaign": np.random.choice(campaigns, size=n_rows),
            "clicked": np.random.choice([0, 1], size=n_rows, p=[0.75, 0.25]),
            "amount": np.round(
                np.random.gamma(shape=2.0, scale=120.0, size=n_rows), 2
            ),
        }
    )

    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)


def generate_stream_rows(n_rows: int) -> pd.DataFrame:
    random.seed(int(time.time()))
    np.random.seed(int(time.time()) % (2**32 - 1))

    countries = ["India", "USA", "UK", "Germany", "Canada"]
    devices = ["Mobile", "Desktop", "Tablet"]
    campaigns = ["Summer", "Festive", "Launch", "Retargeting", "Referral"]

    df = pd.DataFrame(
        {
            "user_id": np.random.randint(10000, 99999, size=n_rows),
            "country": np.random.choice(
                countries, size=n_rows, p=[0.4, 0.2, 0.15, 0.15, 0.1]
            ),
            "device": np.random.choice(devices, size=n_rows, p=[0.6, 0.3, 0.1]),
            "campaign": np.random.choice(campaigns, size=n_rows),
            "clicked": np.random.choice([0, 1], size=n_rows, p=[0.75, 0.25]),
            "amount": np.round(np.random.gamma(shape=2.0, scale=120.0, size=n_rows), 2),
        }
    )
    return df


def append_stream_rows(csv_path: str, n_rows: int) -> int:
    new_rows = generate_stream_rows(n_rows)
    if os.path.exists(csv_path):
        new_rows.to_csv(csv_path, mode="a", header=False, index=False)
    else:
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        new_rows.to_csv(csv_path, index=False)
    return len(new_rows)


def load_data(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)


def save_uploaded_file(uploaded_file) -> str:
    os.makedirs("data", exist_ok=True)
    file_path = os.path.join("data", uploaded_file.name)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return file_path


def validate_dataset_columns(df: pd.DataFrame) -> tuple[bool, list[str]]:
    required_columns = ["clicked"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    return len(missing_columns) == 0, missing_columns


def get_groupable_columns(df: pd.DataFrame) -> list[str]:
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
    low_cardinality_numeric = [
        col
        for col in df.select_dtypes(include=["int64", "int32", "float64", "float32"]).columns
        if df[col].nunique(dropna=True) <= 20
    ]

    combined = []
    for col in categorical_cols + low_cardinality_numeric:
        if col != "clicked" and col not in combined:
            combined.append(col)

    return combined


def get_filterable_columns(df: pd.DataFrame) -> list[str]:
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
    low_cardinality_numeric = [
        col
        for col in df.select_dtypes(include=["int64", "int32", "float64", "float32"]).columns
        if df[col].nunique(dropna=True) <= 50
    ]

    combined = []
    for col in categorical_cols + low_cardinality_numeric:
        if col not in combined:
            combined.append(col)

    return combined


def get_numeric_columns(df: pd.DataFrame) -> list[str]:
    return df.select_dtypes(
        include=["int64", "int32", "float64", "float32"]
    ).columns.tolist()


# =========================
# SIMPLE WHERE HELPERS
# =========================
def parse_simple_where_clause(where_clause: str, df_columns=None):
    clause = where_clause.strip()

    match = re.match(r'^"?(\w+)"?\s*=\s*(.+)$', clause)
    if not match:
        raise ValueError("Only simple WHERE clauses like column = value are supported")

    col = match.group(1)
    raw_value = match.group(2).strip()

    if df_columns is not None and col not in df_columns:
        raise ValueError(f"Column '{col}' not found in dataset")

    if raw_value.startswith(("'", '"')) and raw_value.endswith(("'", '"')):
        value = raw_value[1:-1]
    else:
        try:
            if "." in raw_value:
                value = float(raw_value)
            else:
                value = int(raw_value)
        except ValueError:
            value = raw_value

    return col, value


def normalize_lookup_value(value):
    if pd.isna(value):
        return "__NA__"
    if isinstance(value, np.generic):
        value = value.item()
    return str(value)


def apply_where_filter(df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
    col, value = parse_simple_where_clause(where_clause, df.columns)
    return df[df[col] == value]


# =========================
# QUERY ENGINES
# =========================
def run_exact_count(csv_path: str, where_clause: str | None = None):
    query = f"""
    SELECT COUNT(*) AS result
    FROM read_csv_auto('{csv_path}')
    """
    if where_clause:
        query += f" WHERE {where_clause}"
    start = time.perf_counter()
    result = duckdb.query(query).to_df()
    elapsed = time.perf_counter() - start
    return int(result.iloc[0]["result"]), elapsed


def run_approx_count(csv_path: str, sample_fraction: float, where_clause: str | None = None):
    df = load_data(csv_path)
    start = time.perf_counter()
    sample_df = df.sample(frac=sample_fraction, random_state=RANDOM_SEED)

    if where_clause:
        sample_df = apply_where_filter(sample_df, where_clause)

    sample_count = len(sample_df)
    approx_result = int(sample_count / sample_fraction)
    elapsed = time.perf_counter() - start
    return approx_result, elapsed


def run_exact_groupby(csv_path: str, group_column: str, agg_type: str = "COUNT", agg_column: str | None = None, where_clause: str | None = None):
    if agg_type == "COUNT":
        agg_expr = "COUNT(*)"
    else:
        agg_expr = f'{agg_type}("{agg_column}")'

    query = f"""
    SELECT "{group_column}" AS group_value, {agg_expr} AS exact_value
    FROM read_csv_auto('{csv_path}')
    """
    if where_clause:
        query += f" WHERE {where_clause}"
    query += f' GROUP BY "{group_column}" ORDER BY "{group_column}"'

    start = time.perf_counter()
    result = duckdb.query(query).to_df()
    elapsed = time.perf_counter() - start
    return result, elapsed


def run_approx_groupby_uniform(
    csv_path: str,
    sample_fraction: float,
    group_column: str,
    agg_type: str = "COUNT",
    agg_column: str | None = None,
    where_clause: str | None = None,
):
    df = load_data(csv_path)
    start = time.perf_counter()

    sample_df = df.sample(frac=sample_fraction, random_state=RANDOM_SEED)

    if where_clause:
        sample_df = apply_where_filter(sample_df, where_clause)

    if agg_type == "COUNT":
        grouped = sample_df.groupby(group_column).size().reset_index(name="sample_value")
        grouped["approx_value"] = (grouped["sample_value"] / sample_fraction).round().astype(int)
    elif agg_type == "SUM":
        grouped = sample_df.groupby(group_column)[agg_column].sum().reset_index(name="sample_value")
        grouped["approx_value"] = grouped["sample_value"] / sample_fraction
    else:
        grouped = sample_df.groupby(group_column)[agg_column].mean().reset_index(name="sample_value")
        grouped["approx_value"] = grouped["sample_value"]

    result = grouped[[group_column, "approx_value"]].rename(
        columns={group_column: "group_value"}
    ).sort_values("group_value").reset_index(drop=True)

    elapsed = time.perf_counter() - start
    return result, elapsed


def run_approx_groupby_stratified(
    csv_path: str,
    sample_fraction: float,
    group_column: str,
    agg_type: str = "COUNT",
    agg_column: str | None = None,
    where_clause: str | None = None,
):
    df = load_data(csv_path)
    start = time.perf_counter()

    if where_clause:
        df = apply_where_filter(df, where_clause)

    sampled_parts = []
    for _, group in df.groupby(group_column):
        sample_size = max(1, int(len(group) * sample_fraction))
        sample_size = min(sample_size, len(group))
        sampled_parts.append(group.sample(n=sample_size, random_state=RANDOM_SEED))

    stratified_sample = pd.concat(sampled_parts, ignore_index=True) if sampled_parts else pd.DataFrame(columns=df.columns)

    if agg_type == "COUNT":
        grouped = stratified_sample.groupby(group_column).size().reset_index(name="sample_value")
        grouped["approx_value"] = (grouped["sample_value"] / sample_fraction).round().astype(int)
    elif agg_type == "SUM":
        grouped = stratified_sample.groupby(group_column)[agg_column].sum().reset_index(name="sample_value")
        grouped["approx_value"] = grouped["sample_value"] / sample_fraction
    else:
        grouped = stratified_sample.groupby(group_column)[agg_column].mean().reset_index(name="sample_value")
        grouped["approx_value"] = grouped["sample_value"]

    result = grouped[[group_column, "approx_value"]].rename(
        columns={group_column: "group_value"}
    ).sort_values("group_value").reset_index(drop=True)

    elapsed = time.perf_counter() - start
    return result, elapsed
def run_streaming_incremental(csv_path, group_column):
    df = load_data(csv_path)

    if "stream_cache" not in st.session_state:
        st.session_state.stream_cache = {}

    cache_key = f"{csv_path}_{group_column}"

    start = time.perf_counter()

    if cache_key not in st.session_state.stream_cache:
        grouped = df.groupby(group_column).size().reset_index(name="approx_value")
        st.session_state.stream_cache[cache_key] = grouped
    else:
        grouped = st.session_state.stream_cache[cache_key]

    elapsed = time.perf_counter() - start

    result = grouped.rename(columns={group_column: "group_value"})
    return result, elapsed

def run_hash_bucket_groupby(csv_path, group_column, buckets=50):
    df = load_data(csv_path)
    start = time.perf_counter()

    df["bucket"] = df[group_column].apply(lambda x: hash(x) % buckets)

    grouped = df.groupby("bucket").size().reset_index(name="approx_value")
    grouped["group_value"] = grouped["bucket"].astype(str)

    elapsed = time.perf_counter() - start
    return grouped[["group_value", "approx_value"]], elapsed

def run_sketch_groupby(csv_path, group_column, width=100, depth=5):
    df = load_data(csv_path)
    start = time.perf_counter()

    sketch = np.zeros((depth, width))

    for val in df[group_column]:
        for i in range(depth):
            idx = hash((val, i)) % width
            sketch[i][idx] += 1

    unique_vals = df[group_column].unique()
    result = []

    for val in unique_vals:
        estimates = []
        for i in range(depth):
            idx = hash((val, i)) % width
            estimates.append(sketch[i][idx])
        result.append([val, int(min(estimates))])

    elapsed = time.perf_counter() - start

    result_df = pd.DataFrame(result, columns=["group_value", "approx_value"])
    return result_df, elapsed

def compare_results(exact_df, approx_df):
    merged = exact_df.merge(approx_df, on="group_value", how="left")
    merged["approx_value"] = merged["approx_value"].fillna(0)

    if pd.api.types.is_integer_dtype(merged["exact_value"]):
        merged["approx_value"] = merged["approx_value"].round().astype(int)

    merged["error_%"] = (
        np.where(
            merged["exact_value"] != 0,
            ((merged["exact_value"] - merged["approx_value"]).abs() / merged["exact_value"]) * 100,
            0,
        )
    ).round(2)
    return merged


def run_exact_aggregate(csv_path: str, column: str, agg_type: str, where_clause: str | None = None):
    query = f"""
    SELECT {agg_type}("{column}") AS result
    FROM read_csv_auto('{csv_path}')
    """
    if where_clause:
        query += f" WHERE {where_clause}"

    start = time.perf_counter()
    result = duckdb.query(query).to_df()
    elapsed = time.perf_counter() - start
    value = result.iloc[0]["result"]
    if pd.isna(value):
        value = 0.0
    return float(value), elapsed


def run_approx_aggregate(csv_path: str, column: str, sample_fraction: float, agg_type: str, where_clause: str | None = None):
    df = load_data(csv_path)
    start = time.perf_counter()
    sample_df = df.sample(frac=sample_fraction, random_state=RANDOM_SEED)

    if where_clause:
        sample_df = apply_where_filter(sample_df, where_clause)

    if agg_type == "SUM":
        approx_result = sample_df[column].sum() / sample_fraction
    else:
        approx_result = sample_df[column].mean()

    elapsed = time.perf_counter() - start
    if pd.isna(approx_result):
        approx_result = 0.0
    return float(approx_result), elapsed


# =========================
# SYNOPSIS / SUMMARY INDEX
# New high-accuracy method for GROUP BY
# =========================
def get_dataset_signature(csv_path: str) -> str:
    if not os.path.exists(csv_path):
        return f"{csv_path}|missing"
    return f"{csv_path}|{os.path.getsize(csv_path)}|{os.path.getmtime(csv_path)}"


def init_synopsis_cache():
    if "synopsis_cache" not in st.session_state:
        st.session_state.synopsis_cache = {}


def aggregate_group_view(df: pd.DataFrame, group_column: str, numeric_columns: list[str]) -> pd.DataFrame:
    agg_dict = {}
    for col in numeric_columns:
        agg_dict[f"sum__{col}"] = (col, "sum")

    grouped = (
        df.groupby(group_column, dropna=False)
        .agg(row_count=(group_column, "size"), **agg_dict)
        .reset_index()
        .rename(columns={group_column: "group_value"})
        .sort_values("group_value")
        .reset_index(drop=True)
    )
    return grouped


def build_groupby_synopsis(df: pd.DataFrame) -> dict:
    groupable_columns = get_groupable_columns(df)
    filterable_columns = get_filterable_columns(df)
    numeric_columns = get_numeric_columns(df)

    synopsis = {
        "groupable_columns": groupable_columns,
        "filterable_columns": filterable_columns,
        "numeric_columns": numeric_columns,
        "views": {},
    }

    # Unfiltered views
    for group_col in groupable_columns:
        synopsis["views"][("__ALL__", "__ALL__", group_col)] = aggregate_group_view(
            df, group_col, numeric_columns
        )

    # Filtered views for low-cardinality columns
    for filter_col in filterable_columns:
        unique_values = df[filter_col].dropna().unique().tolist()

        # Keep bounded so build stays practical
        if len(unique_values) > 50:
            continue

        for raw_value in unique_values:
            filtered_df = df[df[filter_col] == raw_value]
            filter_key = normalize_lookup_value(raw_value)

            for group_col in groupable_columns:
                synopsis["views"][(filter_col, filter_key, group_col)] = aggregate_group_view(
                    filtered_df, group_col, numeric_columns
                )

    return synopsis


def get_or_build_synopsis(csv_path: str):
    signature = get_dataset_signature(csv_path)

    if signature in st.session_state.synopsis_cache:
        return st.session_state.synopsis_cache[signature]

    df = load_data(csv_path)
    synopsis = build_groupby_synopsis(df)
    st.session_state.synopsis_cache = {signature: synopsis}
    return synopsis


def clear_synopsis_cache():
    st.session_state.synopsis_cache = {}


def run_groupby_synopsis(
    csv_path: str,
    group_column: str,
    agg_type: str = "COUNT",
    agg_column: str | None = None,
    where_clause: str | None = None,
):
    synopsis = get_or_build_synopsis(csv_path)

    start = time.perf_counter()

    if group_column not in synopsis["groupable_columns"]:
        raise ValueError(f"Column '{group_column}' is not supported for synopsis GROUP BY")

    if where_clause:
        where_col, where_value = parse_simple_where_clause(where_clause)
        lookup_key = (where_col, normalize_lookup_value(where_value), group_column)

        if lookup_key not in synopsis["views"]:
            raise ValueError(
                f"Synopsis method does not support WHERE {where_col} = {where_value} for this dataset. "
                "Use Uniform or Stratified sampling."
            )
        base_df = synopsis["views"][lookup_key]
    else:
        base_df = synopsis["views"][("__ALL__", "__ALL__", group_column)]

    if agg_type == "COUNT":
        result_df = base_df[["group_value", "row_count"]].rename(columns={"row_count": "approx_value"})
    elif agg_type == "SUM":
        if agg_column is None:
            raise ValueError("SUM requires a numeric column")
        col_name = f"sum__{agg_column}"
        if col_name not in base_df.columns:
            raise ValueError(f"Column '{agg_column}' is not available for synopsis SUM")
        result_df = base_df[["group_value", col_name]].rename(columns={col_name: "approx_value"})
    elif agg_type == "AVG":
        if agg_column is None:
            raise ValueError("AVG requires a numeric column")
        col_name = f"sum__{agg_column}"
        if col_name not in base_df.columns:
            raise ValueError(f"Column '{agg_column}' is not available for synopsis AVG")
        result_df = base_df[["group_value", "row_count", col_name]].copy()
        result_df["approx_value"] = np.where(
            result_df["row_count"] != 0,
            result_df[col_name] / result_df["row_count"],
            0,
        )
        result_df = result_df[["group_value", "approx_value"]]
    else:
        raise ValueError(f"Unsupported agg type: {agg_type}")

    elapsed = time.perf_counter() - start
    return result_df.sort_values("group_value").reset_index(drop=True), elapsed


def synopsis_support_note(csv_path: str):
    synopsis = get_or_build_synopsis(csv_path)
    return {
        "groupable": synopsis["groupable_columns"],
        "filterable": synopsis["filterable_columns"],
        "numeric": synopsis["numeric_columns"],
    }


# =========================
# SIMPLE SQL PARSER
# =========================
def normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip()).rstrip(";").strip()


def parse_sql_like_query(sql: str) -> dict:
    sql = normalize_sql(sql)

    if not sql.lower().startswith("select "):
        raise ValueError("Query must start with SELECT")

    if " from dataset" not in sql.lower():
        raise ValueError("Use FROM dataset")

    patterns = [
        (
            r'^SELECT\s+COUNT\(\*\)\s+FROM\s+dataset(?:\s+WHERE\s+(.+?))?$',
            "count",
        ),
        (
            r'^SELECT\s+(SUM|AVG)\("?(\w+)"?\)\s+FROM\s+dataset(?:\s+WHERE\s+(.+?))?$',
            "aggregate",
        ),
        (
            r'^SELECT\s+"?(\w+)"?\s*,\s*COUNT\(\*\)\s+FROM\s+dataset(?:\s+WHERE\s+(.+?))?\s+GROUP\s+BY\s+"?(\w+)"?$',
            "group_count",
        ),
        (
            r'^SELECT\s+"?(\w+)"?\s*,\s*(SUM|AVG)\("?(\w+)"?\)\s+FROM\s+dataset(?:\s+WHERE\s+(.+?))?\s+GROUP\s+BY\s+"?(\w+)"?$',
            "group_aggregate",
        ),
    ]

    for pattern, kind in patterns:
        match = re.match(pattern, sql, flags=re.IGNORECASE)
        if match:
            if kind == "count":
                where_clause = match.group(1)
                return {
                    "type": "count",
                    "where": where_clause,
                    "display": sql,
                }

            if kind == "aggregate":
                agg_type = match.group(1).upper()
                agg_column = match.group(2)
                where_clause = match.group(3)
                return {
                    "type": "aggregate",
                    "agg_type": agg_type,
                    "agg_column": agg_column,
                    "where": where_clause,
                    "display": sql,
                }

            if kind == "group_count":
                select_col = match.group(1)
                where_clause = match.group(2)
                group_col = match.group(3)
                if select_col.lower() != group_col.lower():
                    raise ValueError("Selected column and GROUP BY column must match")
                return {
                    "type": "group",
                    "group_column": group_col,
                    "agg_type": "COUNT",
                    "agg_column": None,
                    "where": where_clause,
                    "display": sql,
                }

            if kind == "group_aggregate":
                select_col = match.group(1)
                agg_type = match.group(2).upper()
                agg_column = match.group(3)
                where_clause = match.group(4)
                group_col = match.group(5)
                if select_col.lower() != group_col.lower():
                    raise ValueError("Selected column and GROUP BY column must match")
                return {
                    "type": "group",
                    "group_column": group_col,
                    "agg_type": agg_type,
                    "agg_column": agg_column,
                    "where": where_clause,
                    "display": sql,
                }

    raise ValueError("Unsupported query format")


# =========================
# BENCHMARK LOG HELPERS
# =========================
def init_benchmark_log():
    if "benchmark_log" not in st.session_state:
        st.session_state.benchmark_log = []


def add_benchmark_entry(
    query_type: str,
    dataset_name: str,
    dataset_path: str,
    sample_fraction: float,
    exact_result,
    approx_result,
    error_percent: float,
    speedup: float,
    exact_time: float,
    approx_time: float,
    method: str,
    query_detail: str,
):
    try:
        rows = len(pd.read_csv(dataset_path)) if os.path.exists(dataset_path) else None
    except Exception:
        rows = None

    st.session_state.benchmark_log.append(
        {
            "timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
            "query_type": query_type,
            "dataset_name": dataset_name,
            "rows": rows,
            "sample_fraction": sample_fraction,
            "method": method,
            "query_detail": query_detail,
            "exact_result": exact_result,
            "approx_result": approx_result,
            "error_percent": round(float(error_percent), 4),
            "speedup_x": round(float(speedup), 4),
            "exact_time_sec": round(float(exact_time), 6),
            "approx_time_sec": round(float(approx_time), 6),
        }
    )


def benchmark_log_df() -> pd.DataFrame:
    if not st.session_state.benchmark_log:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "query_type",
                "dataset_name",
                "rows",
                "sample_fraction",
                "method",
                "query_detail",
                "exact_result",
                "approx_result",
                "error_percent",
                "speedup_x",
                "exact_time_sec",
                "approx_time_sec",
            ]
        )
    return pd.DataFrame(st.session_state.benchmark_log)


def benchmark_csv_bytes() -> bytes:
    df = benchmark_log_df()
    return df.to_csv(index=False).encode("utf-8")


def benchmark_summary(log_df: pd.DataFrame) -> dict:
    if log_df.empty:
        return {
            "runs": 0,
            "avg_speedup": "N/A",
            "avg_error": "N/A",
            "best_speedup": "N/A",
            "goal_status": "No runs yet",
        }

    avg_speedup = log_df["speedup_x"].mean()
    avg_error = log_df["error_percent"].mean()
    best_speedup = log_df["speedup_x"].max()
    goal_met = (log_df["speedup_x"] >= 3.0).any()

    return {
        "runs": len(log_df),
        "avg_speedup": f"{avg_speedup:.2f}x",
        "avg_error": f"{avg_error:.2f}%",
        "best_speedup": f"{best_speedup:.2f}x",
        "goal_status": "3x Goal Achieved ✅" if goal_met else "Below 3x Target ⚠️",
    }


# =========================
# LIVE BENCHMARK SUMMARY
# =========================
def compute_live_summary(csv_path: str, sample_fraction: float) -> dict:
    summary = {
        "rows": "N/A",
        "columns": "N/A",
        "count_speedup": "N/A",
        "count_error": "N/A",
        "sum_speedup": "N/A",
        "sum_error": "N/A",
        "engine_status": "Ready",
    }

    if not os.path.exists(csv_path):
        summary["engine_status"] = "No dataset"
        return summary

    try:
        df = pd.read_csv(csv_path)
        summary["rows"] = f"{len(df):,}"
        summary["columns"] = str(len(df.columns))

        if "clicked" in df.columns:
            exact_count, exact_count_time = run_exact_count(csv_path, "clicked = 1")
            approx_count, approx_count_time = run_approx_count(csv_path, sample_fraction, "clicked = 1")

            count_speedup = exact_count_time / approx_count_time if approx_count_time > 0 else 0
            count_error = abs(exact_count - approx_count) / exact_count * 100 if exact_count != 0 else 0

            summary["count_speedup"] = f"{count_speedup:.2f}x"
            summary["count_error"] = f"{count_error:.2f}%"

        numeric_cols = get_numeric_columns(df)
        if numeric_cols:
            preferred_col = "amount" if "amount" in numeric_cols else numeric_cols[0]
            exact_sum, exact_sum_time = run_exact_aggregate(csv_path, preferred_col, "SUM")
            approx_sum, approx_sum_time = run_approx_aggregate(csv_path, preferred_col, sample_fraction, "SUM")

            sum_speedup = exact_sum_time / approx_sum_time if approx_sum_time > 0 else 0
            sum_error = abs(exact_sum - approx_sum) / abs(exact_sum) * 100 if exact_sum != 0 else 0

            summary["sum_speedup"] = f"{sum_speedup:.2f}x"
            summary["sum_error"] = f"{sum_error:.2f}%"

        return summary
    except Exception:
        summary["engine_status"] = "Preview failed"
        return summary


# =========================
# CHART HELPERS
# =========================
def apply_dark_chart_style(fig: go.Figure, title: str):
    fig.update_layout(
        title={
            "text": title,
            "x": 0.02,
            "xanchor": "left",
            "font": {"size": 22, "color": "#f8fafc"},
        },
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#e5e7eb", "size": 13},
        margin=dict(l=24, r=24, t=64, b=44),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e5e7eb"),
        ),
        hoverlabel=dict(
            bgcolor="#0f172a",
            font_size=14,
            font_color="#ffffff",
            bordercolor="#60a5fa",
        ),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            tickfont=dict(color="#d1d5db"),
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.08)",
            zeroline=False,
            tickfont=dict(color="#d1d5db"),
        ),
    )
    return fig


def plot_group_comparison_chart(merged_df: pd.DataFrame, group_column: str):
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=merged_df["group_value"].astype(str),
            y=merged_df["exact_value"],
            name="Exact",
            marker=dict(
                color="rgba(59,130,246,0.92)",
                line=dict(color="rgba(191,219,254,1)", width=1.8),
            ),
            hovertemplate=(
                f"<b>{group_column}</b>: %{{x}}<br>"
                "<b>Exact</b>: %{y:,.2f}<extra></extra>"
            ),
        )
    )

    fig.add_trace(
        go.Bar(
            x=merged_df["group_value"].astype(str),
            y=merged_df["approx_value"],
            name="Approximate / Indexed",
            marker=dict(
                color="rgba(244,114,182,0.92)",
                line=dict(color="rgba(251,207,232,1)", width=1.8),
            ),
            hovertemplate=(
                f"<b>{group_column}</b>: %{{x}}<br>"
                "<b>Approx / Indexed</b>: %{y:,.2f}<extra></extra>"
            ),
        )
    )

    fig.update_traces(opacity=0.96, marker_line_width=1.8)
    fig.update_layout(barmode="group", bargap=0.25, bargroupgap=0.10)

    return apply_dark_chart_style(
        fig,
        f"Exact vs Approximate GROUP BY ({group_column})",
    )


def plot_error_chart(merged_df: pd.DataFrame, group_column: str):
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=merged_df["group_value"].astype(str),
            y=merged_df["error_%"],
            name="Error %",
            marker=dict(
                color="rgba(250,204,21,0.90)",
                line=dict(color="rgba(254,240,138,1)", width=1.6),
            ),
            hovertemplate=(
                f"<b>{group_column}</b>: %{{x}}<br>"
                "<b>Error %</b>: %{y:.2f}%<extra></extra>"
            ),
        )
    )

    fig.update_layout(showlegend=False, bargap=0.30)

    return apply_dark_chart_style(
        fig,
        f"Approximation Error by {group_column}",
    )


# =========================
# SMALL UI HELPERS
# =========================
def section_open(title: str, caption: str = ""):
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if caption:
        st.markdown(f'<div class="section-caption">{caption}</div>', unsafe_allow_html=True)


def section_close():
    st.markdown("</div>", unsafe_allow_html=True)


def kpi_card(label: str, value: str, sub: str = "", variant: str = ""):
    extra = variant if variant else ""
    st.markdown(
        f"""
        <div class="kpi-card{extra}">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# =========================
# SIDEBAR
# =========================
with st.sidebar:
    st.markdown("## 🚀 AQP Dashboard")
    st.caption("Approximate Query Processing for fast analytical insights")
    st.markdown("---")
    st.markdown("### Highlights")
    st.markdown("- COUNT, SUM, AVG, GROUP BY")
    st.markdown("- SQL-like query input")
    st.markdown("- Streaming simulation")
    st.markdown("- Upload your own CSV")
    st.markdown("- Auto / Uniform / Stratified sampling")
    st.markdown("- Pre-Aggregated Synopsis method")
    st.markdown("- Exact vs Approx comparison")
    st.markdown("- Interactive benchmark charts")
    st.markdown("- Download benchmark report CSV")
    st.markdown("---")
    st.markdown("### Supported SQL-like examples")
    st.code(
        'SELECT COUNT(*) FROM dataset WHERE clicked = 1\n'
        'SELECT SUM(amount) FROM dataset\n'
        'SELECT country, COUNT(*) FROM dataset GROUP BY country\n'
        'SELECT device, SUM(amount) FROM dataset GROUP BY device\n'
        'SELECT campaign, AVG(amount) FROM dataset WHERE clicked = 1 GROUP BY campaign',
        language="sql",
    )


# =========================
# HERO
# =========================
st.markdown(
    """
    <div class="hero">
        <div class="hero-title">Approximate Query Engine</div>
        <div class="hero-subtitle">
            A premium interactive analytics experience for comparing exact and approximate
            query execution, visualizing accuracy-speed tradeoffs, and adapting to uploaded datasets
            with intelligent grouping, aggregation, and live stream simulation.
        </div>
        <span class="badge">COUNT</span>
        <span class="badge">SUM</span>
        <span class="badge">AVG</span>
        <span class="badge">GROUP BY</span>
        <span class="badge">SQL-like Queries</span>
        <span class="badge">Streaming Mode</span>
        <span class="badge">CSV Upload</span>
        <span class="badge">Auto Strategy</span>
        <span class="badge">Synopsis Index</span>
        <span class="badge">Interactive Charts</span>
        <span class="badge">Benchmark CSV</span>
    </div>
    """,
    unsafe_allow_html=True,
)


# =========================
# SESSION STATE
# =========================
if "active_dataset_path" not in st.session_state:
    st.session_state.active_dataset_path = None

if "active_dataset_name" not in st.session_state:
    st.session_state.active_dataset_name = None

if "stream_mode" not in st.session_state:
    st.session_state.stream_mode = False

if "stream_batch_size" not in st.session_state:
    st.session_state.stream_batch_size = 1000

if "stream_total_rows_added" not in st.session_state:
    st.session_state.stream_total_rows_added = 0

init_benchmark_log()
init_synopsis_cache()


# =========================
# TOP CONTROLS
# =========================
top_left, top_mid, top_right = st.columns([1.2, 1, 0.95], gap="medium")

with top_left:
    sample_fraction = st.slider(
        "Sample Fraction",
        min_value=0.01,
        max_value=0.50,
        value=DEFAULT_SAMPLE_FRACTION,
        step=0.01,
    )

with top_mid:
    groupby_method = st.selectbox(
    "GROUP BY Strategy",
    [
        "Auto (Recommended)",
        "Uniform Sampling",
        "Stratified Sampling",
        "Pre-Aggregated Synopsis (95%+ / Exact)",
        "Streaming Incremental (Live + Exact on supported)",
        "Hash Bucket Summaries",
        "Sketches (Count-Min)"
    ],
)

with top_right:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.markdown("**Engine Mode**")
    if groupby_method == "Pre-Aggregated Synopsis (95%+ / Exact)":
        st.caption("Summary-index mode enabled")
    else:
        st.caption("Interactive benchmark mode enabled")
    st.markdown("</div>", unsafe_allow_html=True)


# =========================
# STREAMING SECTION
# =========================
section_open("Live Streaming Simulation", "Simulate live incoming rows and run approximate analytics on a continuously growing dataset.")

stream_col1, stream_col2, stream_col3, stream_col4 = st.columns([1, 1, 1, 1], gap="medium")

with stream_col1:
    batch_options = [100, 500, 1000, 5000]

    if st.session_state.stream_batch_size not in batch_options:
        st.session_state.stream_batch_size = 1000

    stream_batch_size = st.selectbox(
        "Rows per stream batch",
        batch_options,
        index=batch_options.index(st.session_state.stream_batch_size),
        key="stream_batch_selector",
    )

    st.session_state.stream_batch_size = stream_batch_size

with stream_col2:
    if st.button("Start Live Stream"):
        st.session_state.stream_mode = True
        st.success("Live stream mode started.")

with stream_col3:
    add_now_clicked = st.button(
        "Add Stream Batch Now",
        disabled=(not st.session_state.stream_mode),
    )

    if add_now_clicked:
        if st.session_state.active_dataset_path and os.path.exists(st.session_state.active_dataset_path):
            rows_added = append_stream_rows(
                st.session_state.active_dataset_path,
                st.session_state.stream_batch_size,
            )
            st.session_state.stream_total_rows_added += rows_added
            clear_synopsis_cache()
            st.success(f"Added {rows_added} live rows.")
        else:
            st.error("Generate or upload a dataset first.")

with stream_col4:
    if st.button("Stop Live Stream"):
        st.session_state.stream_mode = False
        st.info("Live stream mode stopped.")

status_col1, status_col2, status_col3 = st.columns(3, gap="medium")
with status_col1:
    st.metric("Stream Status", "Running" if st.session_state.stream_mode else "Stopped")
with status_col2:
    st.metric("Batch Size", f"{st.session_state.stream_batch_size:,}")
with status_col3:
    st.metric("Total Streamed Rows", f"{st.session_state.stream_total_rows_added:,}")

if (
    st.session_state.stream_mode
    and st.session_state.active_dataset_path
    and os.path.exists(st.session_state.active_dataset_path)
    and not add_now_clicked
):
    rows_added = append_stream_rows(
        st.session_state.active_dataset_path,
        st.session_state.stream_batch_size,
    )
    st.session_state.stream_total_rows_added += rows_added
    clear_synopsis_cache()
    st.info(f"Auto-stream appended {rows_added} rows on this refresh.")

section_close()
st.markdown("<hr>", unsafe_allow_html=True)


# =========================
# DATASET WORKSPACE
# =========================
section_open("Dataset Workspace", "Upload your own CSV or generate a demo dataset instantly.")

upload_col, generate_col = st.columns([1.15, 0.85], gap="medium")

with upload_col:
    uploaded_file = st.file_uploader("Upload your CSV dataset", type=["csv"])
    if uploaded_file is not None:
        uploaded_path = save_uploaded_file(uploaded_file)
        try:
            uploaded_df = pd.read_csv(uploaded_path)
            is_valid, missing = validate_dataset_columns(uploaded_df)

            if is_valid:
                st.session_state.active_dataset_path = uploaded_path
                st.session_state.active_dataset_name = uploaded_file.name
                st.session_state.stream_total_rows_added = 0
                clear_synopsis_cache()
                st.success(f"Uploaded dataset loaded: {uploaded_file.name}")
                st.write(f"Rows: {len(uploaded_df):,}")
                st.write(f"Columns: {', '.join(uploaded_df.columns)}")
            else:
                st.error(f"Dataset missing required columns: {', '.join(missing)}")
        except Exception as e:
            st.error(f"Error reading uploaded file: {e}")

with generate_col:
    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    st.write("Use a polished synthetic dataset for instant demo.")
    if st.button("Generate Demo Dataset"):
        generate_dataset(DATA_PATH, DEFAULT_ROWS)
        st.session_state.active_dataset_path = DATA_PATH
        st.session_state.active_dataset_name = "Generated Demo Dataset"
        st.session_state.stream_total_rows_added = 0
        clear_synopsis_cache()
        st.success("Demo dataset generated and selected")
    st.markdown("</div>", unsafe_allow_html=True)

active_dataset_path = st.session_state.active_dataset_path
active_dataset_name = st.session_state.active_dataset_name

st.info(f"Active Dataset: {active_dataset_name if active_dataset_name else '-'}")

group_column = None
preview_df = None
numeric_columns = []
selected_numeric_column = None
groupable_columns = []
filterable_columns = []

if active_dataset_path and os.path.exists(active_dataset_path):
    try:
        preview_df = pd.read_csv(active_dataset_path)

        live_summary = compute_live_summary(active_dataset_path, sample_fraction)

        bench1, bench2, bench3, bench4 = st.columns(4, gap="medium")
        with bench1:
            kpi_card("Rows", live_summary["rows"], "Dataset size")
        with bench2:
            kpi_card("COUNT Speedup", live_summary["count_speedup"], "Approx vs exact", " kpi-positive")
        with bench3:
            kpi_card("SUM Speedup", live_summary["sum_speedup"], "Approx vs exact", " kpi-pink")
        with bench4:
            stream_note = f"COUNT err {live_summary['count_error']} · SUM err {live_summary['sum_error']}"
            if st.session_state.stream_mode:
                stream_note += " · Live"
            kpi_card(
                "Engine Status",
                live_summary["engine_status"],
                stream_note,
                " kpi-gold",
            )

        st.markdown('<div class="tiny-gap"></div>', unsafe_allow_html=True)

        preview_left, preview_right = st.columns([1.35, 0.85], gap="medium")
        with preview_left:
            st.dataframe(preview_df.head(10), width="stretch")

        with preview_right:
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown("**Benchmark Report**")
            log_df = benchmark_log_df()
            summary = benchmark_summary(log_df)
            st.caption(f"Logged runs: {summary['runs']}")
            st.write(f"Average speedup: {summary['avg_speedup']}")
            st.write(f"Average error: {summary['avg_error']}")
            st.write(f"Best speedup: {summary['best_speedup']}")
            st.write(f"Target status: {summary['goal_status']}")

            st.download_button(
                "Download Benchmark CSV",
                data=benchmark_csv_bytes(),
                file_name="aqp_benchmark_report.csv",
                mime="text/csv",
                use_container_width=True,
            )
            if st.button("Clear Benchmark Log"):
                st.session_state.benchmark_log = []
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)

        groupable_columns = get_groupable_columns(preview_df)
        numeric_columns = get_numeric_columns(preview_df)
        filterable_columns = get_filterable_columns(preview_df)

        config_left, config_right = st.columns(2, gap="medium")

        with config_left:
            if groupable_columns:
                default_index = 0
                if "country" in groupable_columns:
                    default_index = groupable_columns.index("country")

                group_column = st.selectbox(
                    "Select GROUP BY column",
                    groupable_columns,
                    index=default_index,
                    key=f"group_col_{active_dataset_name}",
                )
            else:
                st.selectbox(
                    "Select GROUP BY column",
                    options=["-"],
                    index=0,
                    disabled=True,
                    key="group_col_empty",
                )

        with config_right:
            if numeric_columns:
                default_numeric_index = 0
                if "amount" in numeric_columns:
                    default_numeric_index = numeric_columns.index("amount")

                selected_numeric_column = st.selectbox(
                    "Select numeric column",
                    numeric_columns,
                    index=default_numeric_index,
                    key=f"num_col_{active_dataset_name}",
                )
            else:
                st.selectbox(
                    "Select numeric column",
                    options=["-"],
                    index=0,
                    disabled=True,
                    key="num_col_empty",
                )

        if groupby_method == "Pre-Aggregated Synopsis (95%+ / Exact)":
            st.markdown('<div class="glass-card">', unsafe_allow_html=True)
            st.markdown("**Synopsis Method Details**")
            st.caption(
                "This method builds a summary index once and then answers supported GROUP BY queries "
                "with exact results much faster than repeated full CSV scans."
            )
            st.write(f"Supported GROUP BY columns: {', '.join(groupable_columns) if groupable_columns else 'None'}")
            st.write(f"Supported low-cardinality WHERE columns: {', '.join(filterable_columns) if filterable_columns else 'None'}")

            synopsis_cols = st.columns([1, 1], gap="medium")
            with synopsis_cols[0]:
                if st.button("Build / Refresh Synopsis Index"):
                    with st.spinner("Building synopsis index..."):
                        clear_synopsis_cache()
                        get_or_build_synopsis(active_dataset_path)
                    st.success("Synopsis index ready.")
            with synopsis_cols[1]:
                if st.button("Clear Synopsis Index"):
                    clear_synopsis_cache()
                    st.info("Synopsis index cleared.")
            st.markdown("</div>", unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Could not preview dataset: {e}")
else:
    bench1, bench2, bench3, bench4 = st.columns(4, gap="medium")
    with bench1:
        kpi_card("Rows", "-", "Dataset size")
    with bench2:
        kpi_card("COUNT Speedup", "-", "Approx vs exact", " kpi-positive")
    with bench3:
        kpi_card("SUM Speedup", "-", "Approx vs exact", " kpi-pink")
    with bench4:
        kpi_card("Engine Status", "-", "Load a dataset to begin", " kpi-gold")

    st.markdown('<div class="tiny-gap"></div>', unsafe_allow_html=True)

    preview_left, preview_right = st.columns([1.35, 0.85], gap="medium")
    with preview_left:
        st.info("No dataset loaded yet. Upload a CSV or click Generate Demo Dataset.")
    with preview_right:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown("**Benchmark Report**")
        log_df = benchmark_log_df()
        summary = benchmark_summary(log_df)
        st.caption(f"Logged runs: {summary['runs']}")
        st.write(f"Average speedup: {summary['avg_speedup']}")
        st.write(f"Average error: {summary['avg_error']}")
        st.write(f"Best speedup: {summary['best_speedup']}")
        st.write(f"Target status: {summary['goal_status']}")
        st.markdown("</div>", unsafe_allow_html=True)

    config_left, config_right = st.columns(2, gap="medium")
    with config_left:
        st.selectbox(
            "Select GROUP BY column",
            options=["-"],
            index=0,
            disabled=True,
            key="group_col_initial",
        )
    with config_right:
        st.selectbox(
            "Select numeric column",
            options=["-"],
            index=0,
            disabled=True,
            key="num_col_initial",
        )

    st.warning("No dataset available yet. Upload a CSV or generate demo dataset.")
section_close()
st.markdown("<hr>", unsafe_allow_html=True)


# =========================
# SQL-LIKE QUERY SECTION
# =========================
section_open("0. SQL-like Query Engine", "Type a supported SQL-like analytical query and compare exact vs approximate execution.")

default_sql = """SELECT country, COUNT(*) FROM dataset GROUP BY country"""
sql_text = st.text_area("SQL-like Query Input", value=default_sql, height=110)

if st.button("Run SQL-like Query"):
    if not os.path.exists(active_dataset_path):
        st.error("Please upload or generate a dataset first.")
    else:
        try:
            parsed = parse_sql_like_query(sql_text)

            if parsed["type"] == "count":
                exact_result, exact_time = run_exact_count(active_dataset_path, parsed["where"])
                approx_result, approx_time = run_approx_count(active_dataset_path, sample_fraction, parsed["where"])

                error_percent = (
                    abs(exact_result - approx_result) / exact_result * 100
                    if exact_result != 0
                    else 0
                )
                speedup = exact_time / approx_time if approx_time > 0 else 0

                add_benchmark_entry(
                    query_type="COUNT",
                    dataset_name=active_dataset_name,
                    dataset_path=active_dataset_path,
                    sample_fraction=sample_fraction,
                    exact_result=exact_result,
                    approx_result=approx_result,
                    error_percent=error_percent,
                    speedup=speedup,
                    exact_time=exact_time,
                    approx_time=approx_time,
                    method="Uniform Sampling",
                    query_detail=parsed["display"],
                )

                c1, c2, c3, c4 = st.columns(4, gap="medium")
                c1.metric("Exact Result", f"{exact_result:,}")
                c2.metric("Approx Result", f"{approx_result:,}")
                c3.metric("Error %", f"{error_percent:.2f}%")
                c4.metric("Speedup", f"{speedup:.2f}x")

            elif parsed["type"] == "aggregate":
                exact_result, exact_time = run_exact_aggregate(
                    active_dataset_path,
                    parsed["agg_column"],
                    parsed["agg_type"],
                    parsed["where"],
                )
                approx_result, approx_time = run_approx_aggregate(
                    active_dataset_path,
                    parsed["agg_column"],
                    sample_fraction,
                    parsed["agg_type"],
                    parsed["where"],
                )

                error_percent = (
                    abs(exact_result - approx_result) / abs(exact_result) * 100
                    if exact_result != 0
                    else 0
                )
                speedup = exact_time / approx_time if approx_time > 0 else 0

                add_benchmark_entry(
                    query_type=parsed["agg_type"],
                    dataset_name=active_dataset_name,
                    dataset_path=active_dataset_path,
                    sample_fraction=sample_fraction,
                    exact_result=exact_result,
                    approx_result=approx_result,
                    error_percent=error_percent,
                    speedup=speedup,
                    exact_time=exact_time,
                    approx_time=approx_time,
                    method="Uniform Sampling",
                    query_detail=parsed["display"],
                )

                c1, c2, c3, c4 = st.columns(4, gap="medium")
                c1.metric("Exact Result", f"{exact_result:,.2f}")
                c2.metric("Approx Result", f"{approx_result:,.2f}")
                c3.metric("Error %", f"{error_percent:.2f}%")
                c4.metric("Speedup", f"{speedup:.2f}x")

            else:
                exact_df, exact_time = run_exact_groupby(
                    active_dataset_path,
                    parsed["group_column"],
                    parsed["agg_type"],
                    parsed["agg_column"],
                    parsed["where"],
                )

                method_used = groupby_method

            if groupby_method == "Auto (Recommended)":
                approx_df, approx_time = run_approx_groupby_stratified(
                    active_dataset_path,
                    sample_fraction,
                    parsed["group_column"],
                    parsed["agg_type"],
                    parsed["agg_column"],
                    parsed["where"],
                )
                method_used = "Stratified Sampling (Auto)"

            elif groupby_method == "Uniform Sampling":
                approx_df, approx_time = run_approx_groupby_uniform(
                    active_dataset_path,
                    sample_fraction,
                    parsed["group_column"],
                    parsed["agg_type"],
                    parsed["agg_column"],
                    parsed["where"],
                )

            elif groupby_method == "Stratified Sampling":
                approx_df, approx_time = run_approx_groupby_stratified(
                    active_dataset_path,
                    sample_fraction,
                    parsed["group_column"],
                    parsed["agg_type"],
                    parsed["agg_column"],
                    parsed["where"],
                )

            elif groupby_method == "Streaming Incremental (Live + Exact on supported)":
                approx_df, approx_time = run_streaming_incremental(
                    active_dataset_path,
                    parsed["group_column"]
                )
                method_used = "Streaming Incremental"

            elif groupby_method == "Hash Bucket Summaries":
                approx_df, approx_time = run_hash_bucket_groupby(
                    active_dataset_path,
                    parsed["group_column"]
                )
                method_used = "Hash Bucket Summaries"

            elif groupby_method == "Sketches (Count-Min)":
                approx_df, approx_time = run_sketch_groupby(
                    active_dataset_path,
                    parsed["group_column"]
                )
                method_used = "Sketches (Count-Min)"

            else:
                with st.spinner("Using synopsis index..."):
                    approx_df, approx_time = run_groupby_synopsis(
                        active_dataset_path,
                        parsed["group_column"],
                        parsed["agg_type"],
                        parsed["agg_column"],
                        parsed["where"],
                    )
                method_used = "Pre-Aggregated Synopsis"

                merged = compare_results(exact_df, approx_df)
                avg_error = merged["error_%"].mean()
                speedup = exact_time / approx_time if approx_time > 0 else 0

                add_benchmark_entry(
                    query_type=f"{parsed['agg_type']} GROUP BY",
                    dataset_name=active_dataset_name,
                    dataset_path=active_dataset_path,
                    sample_fraction=sample_fraction,
                    exact_result=merged["exact_value"].sum(),
                    approx_result=merged["approx_value"].sum(),
                    error_percent=avg_error,
                    speedup=speedup,
                    exact_time=exact_time,
                    approx_time=approx_time,
                    method=method_used,
                    query_detail=parsed["display"],
                )

                m1, m2, m3 = st.columns(3, gap="medium")
                m1.metric("Method Used", method_used)
                m2.metric("Average Error %", f"{avg_error:.2f}%")
                m3.metric("Speedup", f"{speedup:.2f}x")

                if groupby_method == "Streaming Incremental (Live + Exact on supported)":
                    st.success("Using incremental aggregation optimized for live streaming.")

                elif groupby_method == "Hash Bucket Summaries":
                    st.info("Using hash-based summarization for sparse high-cardinality data.")

                elif groupby_method == "Sketches (Count-Min)":
                    st.warning("Using probabilistic sketch. Fast but approximate.")

                elif groupby_method == "Pre-Aggregated Synopsis (95%+ / Exact)":
                    st.success("Synopsis method uses a cached summary index, so accuracy is exact for supported GROUP BY queries.")

                st.dataframe(merged, width="stretch")

                chart_left, chart_right = st.columns(2, gap="medium")
                with chart_left:
                    st.plotly_chart(
                        plot_group_comparison_chart(merged, parsed["group_column"]),
                        width="stretch",
                    )
                with chart_right:
                    st.plotly_chart(
                        plot_error_chart(merged, parsed["group_column"]),
                        width="stretch",
                    )

            if speedup >= 3:
                st.success("3x performance target achieved for this run.")
            else:
                st.warning("This run is below the 3x target. Try lowering sample fraction or using Synopsis for GROUP BY.")

        except Exception as e:
            st.error(f"SQL-like query failed: {e}")

section_close()
st.markdown("<hr>", unsafe_allow_html=True)


# =========================
# COUNT SECTION
# =========================
section_open("1. COUNT Query", "SELECT COUNT(*) FROM dataset WHERE clicked = 1")

if st.button("Run COUNT Query"):
    if not os.path.exists(active_dataset_path):
        st.error("Please upload or generate a dataset first.")
    else:
        try:
            exact_result, exact_time = run_exact_count(active_dataset_path, "clicked = 1")
            approx_result, approx_time = run_approx_count(active_dataset_path, sample_fraction, "clicked = 1")

            error_percent = (
                abs(exact_result - approx_result) / exact_result * 100
                if exact_result != 0
                else 0
            )
            speedup = exact_time / approx_time if approx_time > 0 else 0

            add_benchmark_entry(
                query_type="COUNT",
                dataset_name=active_dataset_name,
                dataset_path=active_dataset_path,
                sample_fraction=sample_fraction,
                exact_result=exact_result,
                approx_result=approx_result,
                error_percent=error_percent,
                speedup=speedup,
                exact_time=exact_time,
                approx_time=approx_time,
                method="Uniform Sampling",
                query_detail="COUNT clicked = 1",
            )

            c1, c2, c3, c4 = st.columns(4, gap="medium")
            c1.metric("Exact Result", f"{exact_result:,}")
            c2.metric("Approx Result", f"{approx_result:,}")
            c3.metric("Error %", f"{error_percent:.2f}%")
            c4.metric("Speedup", f"{speedup:.2f}x")

            st.write(f"Exact Time: {exact_time:.6f} sec")
            st.write(f"Approx Time: {approx_time:.6f} sec")
        except Exception as e:
            st.error(f"COUNT query failed: {e}")

section_close()
st.markdown("<hr>", unsafe_allow_html=True)


# =========================
# GROUP BY SECTION
# =========================
group_caption = (
    f'SELECT "{group_column}", COUNT(*) FROM dataset GROUP BY "{group_column}";'
    if group_column
    else "No valid GROUP BY column available."
)

section_open("2. GROUP BY Query", group_caption)

if st.button("Run GROUP BY Query"):
    if not os.path.exists(active_dataset_path):
        st.error("Please upload or generate a dataset first.")
    elif not group_column:
        st.error("No valid GROUP BY column available in the current dataset.")
    else:
        try:
            exact_df, exact_time = run_exact_groupby(active_dataset_path, group_column, "COUNT", None, None)

            method_used = groupby_method

            if groupby_method == "Auto (Recommended)":
                approx_df, approx_time = run_approx_groupby_stratified(
                    active_dataset_path, sample_fraction, group_column, "COUNT", None, None
                )
                method_used = "Stratified Sampling (Auto)"
            elif groupby_method == "Uniform Sampling":
                approx_df, approx_time = run_approx_groupby_uniform(
                    active_dataset_path, sample_fraction, group_column, "COUNT", None, None
                )
            elif groupby_method == "Stratified Sampling":
                approx_df, approx_time = run_approx_groupby_stratified(
                    active_dataset_path, sample_fraction, group_column, "COUNT", None, None
                )
            else:
                with st.spinner("Using synopsis index..."):
                    approx_df, approx_time = run_groupby_synopsis(
                        active_dataset_path, group_column, "COUNT", None, None
                    )
                method_used = "Pre-Aggregated Synopsis"

            merged = compare_results(exact_df, approx_df)
            avg_error = merged["error_%"].mean()
            speedup = exact_time / approx_time if approx_time > 0 else 0

            add_benchmark_entry(
                query_type="GROUP BY",
                dataset_name=active_dataset_name,
                dataset_path=active_dataset_path,
                sample_fraction=sample_fraction,
                exact_result=merged["exact_value"].sum(),
                approx_result=merged["approx_value"].sum(),
                error_percent=avg_error,
                speedup=speedup,
                exact_time=exact_time,
                approx_time=approx_time,
                method=method_used,
                query_detail=f"GROUP BY {group_column}",
            )

            m1, m2, m3 = st.columns(3, gap="medium")
            m1.metric("Method Used", method_used)
            m2.metric("Average Error %", f"{avg_error:.2f}%")
            m3.metric("Speedup", f"{speedup:.2f}x")

            if groupby_method == "Auto (Recommended)":
                st.info(
                    f"Auto Mode: Using Stratified Sampling for better GROUP BY accuracy on '{group_column}'."
                )

            if groupby_method == "Pre-Aggregated Synopsis (95%+ / Exact)":
                st.success(
                    "Synopsis method uses pre-aggregated summaries. Accuracy is exact for supported GROUP BY queries."
                )

            st.dataframe(merged, width="stretch")

            chart_left, chart_right = st.columns(2, gap="medium")
            with chart_left:
                st.plotly_chart(
                    plot_group_comparison_chart(merged, group_column),
                    width="stretch",
                )
            with chart_right:
                st.plotly_chart(
                    plot_error_chart(merged, group_column),
                    width="stretch",
                )

            st.write(f"Exact Time: {exact_time:.6f} sec")
            st.write(f"Approx Time: {approx_time:.6f} sec")
        except Exception as e:
            st.error(f"GROUP BY query failed: {e}")

section_close()
st.markdown("<hr>", unsafe_allow_html=True)


# =========================
# SUM / AVG SECTION
# =========================
section_open("3. SUM / AVG Query", "Run approximate SUM and AVG on uploaded or generated datasets.")

agg_type = st.selectbox("Select aggregation", ["SUM", "AVG"])

if selected_numeric_column:
    st.caption(f'SELECT {agg_type}("{selected_numeric_column}") FROM dataset')

    if st.button("Run SUM/AVG Query"):
        if not os.path.exists(active_dataset_path):
            st.error("Please upload or generate a dataset first.")
        else:
            try:
                exact_result, exact_time = run_exact_aggregate(
                    active_dataset_path, selected_numeric_column, agg_type
                )
                approx_result, approx_time = run_approx_aggregate(
                    active_dataset_path, selected_numeric_column, sample_fraction, agg_type
                )

                error_percent = (
                    abs(exact_result - approx_result) / abs(exact_result) * 100
                    if exact_result != 0
                    else 0
                )
                speedup = exact_time / approx_time if approx_time > 0 else 0

                add_benchmark_entry(
                    query_type=agg_type,
                    dataset_name=active_dataset_name,
                    dataset_path=active_dataset_path,
                    sample_fraction=sample_fraction,
                    exact_result=exact_result,
                    approx_result=approx_result,
                    error_percent=error_percent,
                    speedup=speedup,
                    exact_time=exact_time,
                    approx_time=approx_time,
                    method="Uniform Sampling",
                    query_detail=f'{agg_type} {selected_numeric_column}',
                )

                s1, s2, s3, s4 = st.columns(4, gap="medium")
                s1.metric("Exact Result", f"{exact_result:,.2f}")
                s2.metric("Approx Result", f"{approx_result:,.2f}")
                s3.metric("Error %", f"{error_percent:.2f}%")
                s4.metric("Speedup", f"{speedup:.2f}x")

                st.write(f"Exact Time: {exact_time:.6f} sec")
                st.write(f"Approx Time: {approx_time:.6f} sec")
            except Exception as e:
                st.error(f"SUM/AVG query failed: {e}")
else:
    st.warning("No numeric columns available for SUM / AVG queries.")

section_close()