import argparse
import json
import math
import os
import sys
from itertools import product
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from cmdstanpy import cmdstan_path, install_cmdstan
except ModuleNotFoundError:  # pragma: no cover - container fallback
    cmdstan_path = None
    install_cmdstan = None

from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.serialize import model_to_json


SCRIPT_ROOT = Path(__file__).resolve()
REPO_ROOT = next(
    (candidate for candidate in (SCRIPT_ROOT.parents[2], SCRIPT_ROOT.parents[1]) if (candidate / 'Testing Data').exists()),
    SCRIPT_ROOT.parents[1],
)
DEFAULT_WORKBOOK = REPO_ROOT / 'Testing Data' / 'CIVORA_5Y_Retail_Data.xlsx'
DEFAULT_PROPHET_READY_WORKBOOK = REPO_ROOT / 'Testing Data' / 'CIVORA_Prophet_Ready_FINAL.xlsx'
DEFAULT_HOLIDAYS_WORKBOOK = (
    DEFAULT_PROPHET_READY_WORKBOOK
    if DEFAULT_PROPHET_READY_WORKBOOK.exists()
    else (REPO_ROOT / 'Testing Data' / 'India_Government_and_Regional_Holidays_2020_2030.xlsx')
)
DEFAULT_PROMOTIONS_WORKBOOK = (
    DEFAULT_PROPHET_READY_WORKBOOK
    if DEFAULT_PROPHET_READY_WORKBOOK.exists()
    else DEFAULT_WORKBOOK
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / 'backend' / 'models' / 'prophet'
DEFAULT_SHEET = 'Daily_Revenue_Prophet'
MONTH_NAME_TO_INDEX = {
    'january': 1,
    'february': 2,
    'march': 3,
    'april': 4,
    'may': 5,
    'june': 6,
    'july': 7,
    'august': 8,
    'september': 9,
    'october': 10,
    'november': 11,
    'december': 12,
}
HOLIDAY_SHEET_CANDIDATES = (
    'Prophet_Holidays',
    'Central_2020_2025',
    'Tamil_Nadu_2020_2025',
    'Holidays_2026',
    'Holidays',
    'Holiday_Master',
    'Holiday_Data',
    'Government_Holidays',
    'Gov_Holidays',
    'File_Holidays',
)
PROMOTION_SHEET_CANDIDATES = (
    'Promotion_Master_Clean',
    'Promotions_Template',
    'Promotions',
    'Promotion_Master',
    'Promos',
)

HOLIDAY_REGRESSORS = ['holiday_active', 'holiday_count', 'festival_holiday_count']
PROMOTION_REGRESSORS = [
    'promo_active',
    'promo_count',
    'promo_days',
    'promo_discount_max',
    'festival_promo_days',
    'seasonal_promo_days',
    'clearance_promo_days',
    'flash_sale_days',
]


def load_revenue_frame(workbook_path: Path, sheet_name: str) -> pd.DataFrame:
    excel = pd.ExcelFile(workbook_path)

    if sheet_name in excel.sheet_names:
        frame = pd.read_excel(workbook_path, sheet_name=sheet_name)
        if {'ds', 'y'}.issubset(frame.columns):
            return frame[['ds', 'y']].copy()

    if 'Sales_Data' in excel.sheet_names:
        frame = pd.read_excel(workbook_path, sheet_name='Sales_Data')
        if {'ds', 'Revenue'}.issubset(frame.columns):
            aggregated = (
                frame[['ds', 'Revenue']]
                .copy()
                .rename(columns={'Revenue': 'y'})
                .groupby('ds', as_index=False)['y']
                .sum()
            )
            return aggregated

    raise ValueError(
        f'No revenue time-series sheet found. Expected "{sheet_name}" or a Sales_Data sheet with ds/Revenue columns.'
    )


def load_profit_frame(workbook_path: Path, sheet_name: str = 'Daily_Profit_Prophet') -> pd.DataFrame:
    excel = pd.ExcelFile(workbook_path)

    if sheet_name in excel.sheet_names:
        frame = pd.read_excel(workbook_path, sheet_name=sheet_name)
        if {'ds', 'y'}.issubset(frame.columns):
            return frame[['ds', 'y']].copy()

    if 'Sales_Data' in excel.sheet_names:
        frame = pd.read_excel(workbook_path, sheet_name='Sales_Data')
        if {'ds', 'Revenue', 'Cost'}.issubset(frame.columns):
            frame['Profit'] = pd.to_numeric(frame['Revenue'], errors='coerce').fillna(0) - pd.to_numeric(frame['Cost'], errors='coerce').fillna(0)
            aggregated = (
                frame[['ds', 'Profit']]
                .copy()
                .rename(columns={'Profit': 'y'})
                .groupby('ds', as_index=False)['y']
                .sum()
            )
            return aggregated
        elif {'ds', 'Profit'}.issubset(frame.columns):
            aggregated = (
                frame[['ds', 'Profit']]
                .copy()
                .rename(columns={'Profit': 'y'})
                .groupby('ds', as_index=False)['y']
                .sum()
            )
            return aggregated

    raise ValueError(
        f'No profit time-series sheet found. Expected "{sheet_name}" or a Sales_Data sheet with Revenue/Cost or Profit columns.'
    )


def load_cost_frame(workbook_path: Path, sheet_name: str = 'Daily_Cost_Prophet') -> pd.DataFrame:
    excel = pd.ExcelFile(workbook_path)

    if sheet_name in excel.sheet_names:
        frame = pd.read_excel(workbook_path, sheet_name=sheet_name)
        if {'ds', 'y'}.issubset(frame.columns):
            return frame[['ds', 'y']].copy()

    if 'Sales_Data' in excel.sheet_names:
        frame = pd.read_excel(workbook_path, sheet_name='Sales_Data')
        if {'ds', 'Cost'}.issubset(frame.columns):
            aggregated = (
                frame[['ds', 'Cost']]
                .copy()
                .rename(columns={'Cost': 'y'})
                .groupby('ds', as_index=False)['y']
                .sum()
            )
            return aggregated

    raise ValueError(
        f'No cost time-series sheet found. Expected "{sheet_name}" or a Sales_Data sheet with Cost column.'
    )


def load_holidays_frame(
    workbook_path: Path,
    min_year: int | None = None,
    max_year: int | None = None,
) -> pd.DataFrame | None:
    candidate_paths = [workbook_path]
    if DEFAULT_PROPHET_READY_WORKBOOK.exists() and DEFAULT_PROPHET_READY_WORKBOOK not in candidate_paths:
        candidate_paths.insert(0, DEFAULT_PROPHET_READY_WORKBOOK)
    if DEFAULT_HOLIDAYS_WORKBOOK.exists() and DEFAULT_HOLIDAYS_WORKBOOK not in candidate_paths:
        candidate_paths.append(DEFAULT_HOLIDAYS_WORKBOOK)

    holiday_frames = []

    for path in candidate_paths:
        if not path.exists():
            continue
        try:
            excel = pd.ExcelFile(path)
        except Exception:
            continue

        for holiday_sheet in HOLIDAY_SHEET_CANDIDATES:
            if holiday_sheet not in excel.sheet_names:
                continue

            frame = pd.read_excel(path, sheet_name=holiday_sheet)
            if frame.empty:
                continue

            lower_columns = {str(column).strip().lower(): column for column in frame.columns}
            date_column = next(
                (lower_columns[key] for key in ('ds', 'date', 'holiday_date', 'day') if key in lower_columns),
                None,
            )
            name_column = next(
                (lower_columns[key] for key in ('holiday', 'holiday_name', 'name', 'title', 'event') if key in lower_columns),
                None,
            )

            if not date_column:
                continue

            scope_value = None
            if 'scope' in lower_columns:
                scope_value = frame[lower_columns['scope']].astype(str).str.strip()

            holidays = pd.DataFrame({
                'ds': pd.to_datetime(frame[date_column], errors='coerce'),
                'holiday': frame[name_column].astype(str).str.strip() if name_column else holiday_sheet,
            }).dropna(subset=['ds'])

            if holidays.empty:
                continue

            if scope_value is not None:
                holidays['holiday'] = (
                    scope_value.fillna(holiday_sheet).astype(str).str.strip().replace({'nan': holiday_sheet, 'None': holiday_sheet})
                    + ' - '
                    + holidays['holiday'].fillna(holiday_sheet).astype(str).str.strip().replace({'nan': holiday_sheet, 'None': holiday_sheet})
                )

            holidays['holiday'] = holidays['holiday'].replace({'nan': holiday_sheet, 'None': holiday_sheet}).fillna(holiday_sheet)
            holidays['lower_window'] = -2
            holidays['upper_window'] = 1

            if min_year is not None:
                holidays = holidays[holidays['ds'].dt.year >= int(min_year)]
            if max_year is not None:
                holidays = holidays[holidays['ds'].dt.year <= int(max_year)]

            holiday_frames.append(holidays[['ds', 'holiday', 'lower_window', 'upper_window']])

    if not holiday_frames:
        return None

    holidays = (
        pd.concat(holiday_frames, ignore_index=True)
        .drop_duplicates(subset=['ds', 'holiday'])
        .sort_values(['ds', 'holiday'])
        .reset_index(drop=True)
    )
    return holidays if not holidays.empty else None


def load_promotions_frame(
    workbook_path: Path | None = None,
    store_id: int | str | None = None,
    store_name: str | None = None,
) -> pd.DataFrame:
    candidate_paths = []
    if workbook_path is not None:
        candidate_paths.append(workbook_path)
    if DEFAULT_PROPHET_READY_WORKBOOK.exists() and DEFAULT_PROPHET_READY_WORKBOOK not in candidate_paths:
        candidate_paths.append(DEFAULT_PROPHET_READY_WORKBOOK)
    if DEFAULT_WORKBOOK.exists() and DEFAULT_WORKBOOK not in candidate_paths:
        candidate_paths.append(DEFAULT_WORKBOOK)

    promo_frames = []

    for path in candidate_paths:
        if not path.exists():
            continue
        try:
            excel = pd.ExcelFile(path)
        except Exception:
            continue

        for sheet in PROMOTION_SHEET_CANDIDATES:
            if sheet not in excel.sheet_names:
                continue

            frame = pd.read_excel(path, sheet_name=sheet)
            if frame.empty:
                continue

            lower_columns = {str(col).strip().lower(): col for col in frame.columns}
            start_col = next((lower_columns[k] for k in ('start_date', 'start', 'startdate') if k in lower_columns), None)
            end_col = next((lower_columns[k] for k in ('end_date', 'end', 'enddate') if k in lower_columns), None)
            id_col = next((lower_columns[k] for k in ('promotion_id', 'promo_id', 'id') if k in lower_columns), None)
            store_col = next((lower_columns[k] for k in ('store', 'store_id', 'store_name') if k in lower_columns), None)
            name_col = next((lower_columns[k] for k in ('promotion_name', 'promo_name', 'name') if k in lower_columns), None)
            type_col = next((lower_columns[k] for k in ('promotion_type', 'promo_type', 'type') if k in lower_columns), None)
            disc_col = next((lower_columns[k] for k in ('discount_percent', 'discount', 'discount_pct') if k in lower_columns), None)

            if not (start_col and end_col):
                continue

            promos = pd.DataFrame({
                'Promotion_ID': frame[id_col].astype(str) if id_col else [f'PROMO-{idx:03d}' for idx in range(1, len(frame) + 1)],
                'Store': frame[store_col].astype(str).str.strip() if store_col else 'ALL',
                'Start_Date': pd.to_datetime(frame[start_col], errors='coerce'),
                'End_Date': pd.to_datetime(frame[end_col], errors='coerce'),
                'Promotion_Name': frame[name_col].astype(str).str.strip() if name_col else 'Promotion',
                'Promotion_Type': frame[type_col].astype(str).str.strip() if type_col else 'General',
                'Discount_Percent': pd.to_numeric(frame[disc_col], errors='coerce').fillna(0) if disc_col else 0,
            }).dropna(subset=['Start_Date', 'End_Date'])

            if not promos.empty:
                promo_frames.append(promos)
                break

        if promo_frames:
            break

    if not promo_frames:
        return pd.DataFrame(columns=[
            'Promotion_ID', 'Store', 'Start_Date', 'End_Date',
            'Promotion_Name', 'Promotion_Type', 'Discount_Percent'
        ])

    all_promos = pd.concat(promo_frames, ignore_index=True)
    all_promos['Store_Norm'] = all_promos['Store'].str.upper().str.strip()

    # Store filtering logic: Keep promotions where Store == 'ALL' or Store == current_store_id or Store == current_store_name
    if store_id is not None or store_name is not None:
        target_store_ids = {str(store_id).strip().upper(), f'PROJECT_{store_id}'.upper(), f'STORE_{store_id}'.upper(), f'STORE {store_id}'.upper()} if store_id is not None else set()
        target_store_names = {str(store_name).strip().upper()} if store_name else set()
        match_targets = {'ALL', *target_store_ids, *target_store_names}
        filtered_promos = all_promos[all_promos['Store_Norm'].isin(match_targets)].copy()
    else:
        filtered_promos = all_promos.copy()

    # Sort and preserve all overlapping promotions
    filtered_promos = (
        filtered_promos.drop(columns=['Store_Norm'], errors='ignore')
        .sort_values(['Start_Date', 'End_Date', 'Promotion_ID'])
        .reset_index(drop=True)
    )
    return filtered_promos


def build_monthly_event_features(
    date_series: pd.Series | pd.DatetimeIndex,
    holidays_df: pd.DataFrame | None = None,
    promotions_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    months = pd.to_datetime(pd.Series(date_series)).dt.to_period('M').dt.to_timestamp().drop_duplicates().sort_values()
    rows = []

    festive_keywords = (
        'diwali', 'deepavali', 'pongal', 'dussehra', 'dasara', 'durga puja',
        'eid', 'id-ul', 'idul', 'christmas', 'holi', 'navratri', 'onam',
        'mahavir', 'good friday', 'buddha purnima', 'ganesh', 'vinayakar',
        'janmashtami', 'raksha', 'new year', 'festival'
    )

    for start_dt in months:
        end_dt = start_dt + pd.offsets.MonthEnd(1)

        # 1. Holidays Feature Engineering
        h_active = 0
        h_count = 0
        fest_h_count = 0
        nat_h_count = 0

        if holidays_df is not None and not holidays_df.empty:
            h_in_m = holidays_df[(holidays_df['ds'] >= start_dt) & (holidays_df['ds'] <= end_dt)]
            h_count = len(h_in_m)
            h_active = 1 if h_count > 0 else 0
            if h_count > 0:
                h_names = h_in_m['holiday'].astype(str).str.lower()
                fest_h_count = int(h_names.apply(lambda name: any(kw in name for kw in festive_keywords)).sum())
                nat_h_count = int(h_names.apply(lambda name: any(kw in name for kw in ('republic', 'independence', 'gandhi', 'national'))).sum())

        # 2. Promotions Feature Engineering
        p_active = 0
        p_count = 0
        p_disc_max = 0
        m_days = set()
        fest_days = set()
        seas_days = set()
        clear_days = set()
        flash_days = set()

        if promotions_df is not None and not promotions_df.empty:
            p_in_m = promotions_df[(promotions_df['Start_Date'] <= end_dt) & (promotions_df['End_Date'] >= start_dt)]
            p_count = len(p_in_m)
            p_active = 1 if p_count > 0 else 0
            if p_count > 0:
                p_disc_max = int(pd.to_numeric(p_in_m['Discount_Percent'], errors='coerce').fillna(0).max())
                for _, promo in p_in_m.iterrows():
                    p_start = max(start_dt, pd.to_datetime(promo['Start_Date']))
                    p_end = min(end_dt, pd.to_datetime(promo['End_Date']))
                    cur_days = set(pd.date_range(p_start, p_end, freq='D'))
                    m_days.update(cur_days)

                    p_type = str(promo['Promotion_Type']).strip().lower()
                    if 'festival' in p_type or 'diwali' in p_type:
                        fest_days.update(cur_days)
                    elif 'seasonal' in p_type or 'summer' in p_type or 'new year' in p_type:
                        seas_days.update(cur_days)
                    elif 'clearance' in p_type or 'year-end' in p_type:
                        clear_days.update(cur_days)
                    elif 'flash' in p_type or 'weekend' in p_type:
                        flash_days.update(cur_days)

        rows.append({
            'ds': start_dt,
            'holiday_active': h_active,
            'holiday_count': h_count,
            'festival_holiday_count': fest_h_count,
            'national_holiday_count': nat_h_count,
            'promo_active': p_active,
            'promo_count': p_count,
            'promo_days': len(m_days),
            'promo_discount_max': p_disc_max,
            'festival_promo_days': len(fest_days),
            'seasonal_promo_days': len(seas_days),
            'clearance_promo_days': len(clear_days),
            'flash_sale_days': len(flash_days),
            'festival_promo_active': 1 if len(fest_days) > 0 else 0,
            'seasonal_promo_active': 1 if len(seas_days) > 0 else 0,
            'clearance_promo_active': 1 if len(clear_days) > 0 else 0,
            'flash_sale_active': 1 if len(flash_days) > 0 else 0,
        })

    return pd.DataFrame(rows).sort_values('ds').reset_index(drop=True)


def merge_event_features(sales_df: pd.DataFrame, event_features_df: pd.DataFrame, allow_negative_y: bool = False, metric_name: str = 'metric') -> pd.DataFrame:
    rows_before = len(sales_df)
    merged = pd.merge(sales_df, event_features_df, on='ds', how='left')
    rows_after = len(merged)

    # Strict Join Safety Assertions
    if rows_before != rows_after:
        raise ValueError(
            f'Join safety failure: row count before join ({rows_before}) does not match row count after join ({rows_after}).'
        )

    if merged['ds'].duplicated().any():
        dup_dates = merged[merged['ds'].duplicated()]['ds'].tolist()
        raise ValueError(f'Join safety failure: duplicate ds detected after merge: {dup_dates}')

    if merged['ds'].isna().any():
        raise ValueError('Join safety failure: missing ds values detected after merge.')

    if merged['y'].isna().any() or not np.all(np.isfinite(merged['y'])):
        raise ValueError('Join safety failure: non-finite or missing y values detected.')

    if not allow_negative_y and (merged['y'] < 0).any():
        raise ValueError(f'Join safety failure: negative {metric_name} values detected.')

    # Fill any missing event features with documented 0 default (meaning "no known event")
    for col in HOLIDAY_REGRESSORS + PROMOTION_REGRESSORS:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0).astype(float)

    return merged


def prepare_daily_series(frame: pd.DataFrame, allow_negative_y: bool = False, metric_name: str = 'metric') -> pd.DataFrame:
    cleaned = frame.copy()
    cleaned['ds'] = pd.to_datetime(cleaned['ds'], errors='coerce')
    cleaned['y'] = pd.to_numeric(cleaned['y'], errors='coerce')
    cleaned = cleaned.dropna(subset=['ds', 'y'])
    cleaned = cleaned.groupby('ds', as_index=False)['y'].sum().sort_values('ds')

    if cleaned.empty:
        raise ValueError(f'{metric_name} series is empty after cleaning.')

    full_range = pd.date_range(cleaned['ds'].min(), cleaned['ds'].max(), freq='D')
    cleaned = (
        cleaned.set_index('ds')
        .reindex(full_range)
        .rename_axis('ds')
    )
    
    # Interpolate: for non-negative metrics, clip to 0; for metrics that can be negative (profit), don't clip
    if allow_negative_y:
        cleaned['y'] = cleaned['y'].interpolate(method='time').ffill().bfill()
    else:
        cleaned['y'] = cleaned['y'].interpolate(method='time').ffill().bfill().clip(lower=0)
    
    cleaned = cleaned.reset_index()

    return cleaned


def prepare_monthly_series(frame: pd.DataFrame, allow_negative_y: bool = False, metric_name: str = 'metric') -> pd.DataFrame:
    cleaned = frame.copy()
    cleaned['ds'] = pd.to_datetime(cleaned['ds'], errors='coerce')
    cleaned['y'] = pd.to_numeric(cleaned['y'], errors='coerce')
    cleaned = cleaned.dropna(subset=['ds', 'y'])
    cleaned['ds'] = cleaned['ds'].dt.to_period('M').dt.to_timestamp()
    cleaned = cleaned.groupby('ds', as_index=False)['y'].sum().sort_values('ds')

    if cleaned.empty:
        raise ValueError(f'{metric_name} series is empty after cleaning.')

    full_range = pd.date_range(cleaned['ds'].min(), cleaned['ds'].max(), freq='MS')
    cleaned = (
        cleaned.set_index('ds')
        .reindex(full_range)
        .rename_axis('ds')
    )
    
    # Interpolate: for non-negative metrics, clip to 0; for metrics that can be negative (profit), don't clip
    if allow_negative_y:
        cleaned['y'] = cleaned['y'].interpolate(method='time').ffill().bfill()
    else:
        cleaned['y'] = cleaned['y'].interpolate(method='time').ffill().bfill().clip(lower=0)
    
    cleaned = cleaned.reset_index()

    return cleaned


def log1p_series(frame: pd.DataFrame) -> pd.DataFrame:
    transformed = frame.copy()
    transformed['y'] = pd.to_numeric(transformed['y'], errors='coerce').fillna(0).clip(lower=0).apply(math.log1p)
    return transformed


def inverse_log1p_series(values: pd.Series) -> pd.Series:
    numeric_values = pd.to_numeric(values, errors='coerce').fillna(0).astype(float)
    return numeric_values.apply(lambda value: max(0.0, math.expm1(value)))


def restore_original_scale(frame: pd.DataFrame, value_columns: tuple[str, ...]) -> pd.DataFrame:
    restored = frame.copy()
    for column in value_columns:
        if column in restored.columns:
            restored[column] = inverse_log1p_series(restored[column])
    return restored


def load_project_name_from_database(project_id: int | None = None) -> str | None:
    if project_id is None:
        return None

    try:
        import mysql.connector
    except ModuleNotFoundError as import_error:
        raise ModuleNotFoundError(
            'mysql-connector-python is required to train from the database source.'
        ) from import_error

    connection = mysql.connector.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'user'),
        password=os.getenv('DB_PASSWORD', 'password'),
        database=os.getenv('DB_NAME', 'datavis_db'),
    )

    try:
        query = 'SELECT name FROM projects WHERE id = %s'
        frame = pd.read_sql(query, connection, params=[project_id])
    finally:
        connection.close()

    if frame.empty:
        return None

    return str(frame.iloc[0]['name']).strip() or None


def load_metric_frame_from_database(metric_column: str = 'total_revenue', project_id: int | None = None) -> pd.DataFrame:
    try:
        import mysql.connector
    except ModuleNotFoundError as import_error:
        raise ModuleNotFoundError(
            'mysql-connector-python is required to train from the database source.'
        ) from import_error

    connection = mysql.connector.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'user'),
        password=os.getenv('DB_PASSWORD', 'password'),
        database=os.getenv('DB_NAME', 'datavis_db'),
    )

    try:
        query = f"""
            SELECT ss.year, ss.month_name, SUM(ss.{metric_column}) AS y
            FROM sales_summaries ss
            INNER JOIN projects p ON p.id = ss.project_id
        """
        params = []
        if project_id is not None:
            query += ' WHERE p.id = %s'
            params.append(project_id)
        query += """
            GROUP BY ss.year, ss.month_name
            ORDER BY ss.year ASC, FIELD(ss.month_name, 'January', 'February', 'March', 'April', 'May', 'June',
                                        'July', 'August', 'September', 'October', 'November', 'December')
        """
        frame = pd.read_sql(query, connection, params=params)
    finally:
        connection.close()

    if frame.empty:
        raise ValueError('No imported revenue data was found in the database.')

    if len(frame) < 2:
        raise ValueError(f'Store has only {len(frame)} monthly record. At least 2 monthly records are required for forecasting.')

    frame['month_name'] = frame['month_name'].astype(str).str.strip().str.lower()
    frame['month_index'] = frame['month_name'].map(MONTH_NAME_TO_INDEX)
    frame['ds'] = pd.to_datetime(
        {
            'year': pd.to_numeric(frame['year'], errors='coerce'),
            'month': frame['month_index'],
            'day': 1,
        },
        errors='coerce',
    )
    frame['y'] = pd.to_numeric(frame['y'], errors='coerce')
    frame = frame.dropna(subset=['ds', 'y'])

    return frame[['ds', 'y']].copy()


def load_cost_frame_from_database(project_id: int | None = None) -> pd.DataFrame:
    """Load monthly cost data from database (total_cost column from sales_summaries)."""
    try:
        import mysql.connector
    except ModuleNotFoundError as import_error:
        raise ModuleNotFoundError(
            'mysql-connector-python is required to train from the database source.'
        ) from import_error

    connection = mysql.connector.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'user'),
        password=os.getenv('DB_PASSWORD', 'password'),
        database=os.getenv('DB_NAME', 'datavis_db'),
    )

    try:
        query = """
            SELECT ss.year, ss.month_name, SUM(ss.total_cost) AS y
            FROM sales_summaries ss
            INNER JOIN projects p ON p.id = ss.project_id
        """
        params = []
        if project_id is not None:
            query += ' WHERE p.id = %s'
            params.append(project_id)
        query += """
            GROUP BY ss.year, ss.month_name
            ORDER BY ss.year ASC, FIELD(ss.month_name, 'January', 'February', 'March', 'April', 'May', 'June',
                                        'July', 'August', 'September', 'October', 'November', 'December')
        """
        frame = pd.read_sql(query, connection, params=params)
    finally:
        connection.close()

    if frame.empty:
        raise ValueError('No cost data was found in the database.')

    if len(frame) < 2:
        raise ValueError(f'Store has only {len(frame)} monthly cost record. At least 2 monthly records are required for forecasting.')

    frame['month_name'] = frame['month_name'].astype(str).str.strip().str.lower()
    frame['month_index'] = frame['month_name'].map(MONTH_NAME_TO_INDEX)
    frame['ds'] = pd.to_datetime(
        {
            'year': pd.to_numeric(frame['year'], errors='coerce'),
            'month': frame['month_index'],
            'day': 1,
        },
        errors='coerce',
    )
    frame['y'] = pd.to_numeric(frame['y'], errors='coerce')
    frame = frame.dropna(subset=['ds', 'y'])

    return frame[['ds', 'y']].copy()


def split_train_test(series: pd.DataFrame, test_days: int, granularity: str = 'daily') -> tuple[pd.DataFrame, pd.DataFrame]:
    granularity = granularity.lower().strip()
    min_points = 2 if granularity == 'monthly' else 30
    if len(series) < min_points:
        raise ValueError(f'Not enough revenue history to train Prophet for {granularity} data (found {len(series)} points, minimum is {min_points}).')

    if granularity == 'monthly':
        if len(series) >= 24:
            actual_test = min(6, max(2, len(series) // 6))
        elif len(series) >= 12:
            actual_test = min(3, max(1, len(series) // 5))
        elif len(series) >= 6:
            actual_test = 2
        else:
            actual_test = 1
        test_days = min(actual_test, max(1, len(series) - 1))
    else:
        if len(series) <= test_days + 30:
            test_days = max(14, int(len(series) * 0.2))

    train = series.iloc[:-test_days].copy()
    test = series.iloc[-test_days:].copy()
    return train, test


def ensure_cmdstan():
    if cmdstan_path is None or install_cmdstan is None:
        return

    try:
        cmdstan_path()
        return
    except Exception:
        if os.getenv('CIVORA_INSTALL_CMDSTAN') == '1':
            print('CmdStan is missing. Installing it once for Prophet...', flush=True)
            install_cmdstan(cores=max(1, min(4, os.cpu_count() or 1)))


def build_model() -> Prophet:
    return build_model_with_params()


def build_model_with_params(
    changepoint_prior_scale: float = 0.05,
    seasonality_prior_scale: float = 10.0,
    holidays_prior_scale: float = 10.0,
    holidays: pd.DataFrame | None = None,
    granularity: str = 'daily',
    data_points: int = 100,
    regressors: list[str] | None = None,
) -> Prophet:
    granularity = granularity.lower().strip()
    if granularity == 'monthly':
        # Monthly model: Raw additive trend + conservative annual seasonality (Fourier order 2)
        # Weekly, daily, and quarterly seasonalities are strictly disabled
        model = Prophet(
            yearly_seasonality=False,
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.9,
            changepoint_prior_scale=changepoint_prior_scale,
            seasonality_prior_scale=seasonality_prior_scale,
            holidays_prior_scale=holidays_prior_scale,
            holidays=holidays if (holidays is not None and not regressors) else None,
            changepoint_range=0.9,
        )
        if data_points >= 12:
            model.add_seasonality(name='yearly', period=365.25, fourier_order=2)

        if regressors:
            for regressor in regressors:
                model.add_regressor(regressor)

        return model

    model = Prophet(
        yearly_seasonality=12,
        weekly_seasonality=True,
        daily_seasonality=False,
        interval_width=0.9,
        changepoint_prior_scale=changepoint_prior_scale,
        seasonality_prior_scale=seasonality_prior_scale,
        holidays_prior_scale=holidays_prior_scale,
        holidays=holidays,
        changepoint_range=0.9,
    )
    model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
    model.add_seasonality(name='quarterly', period=91.25, fourier_order=5)

    if regressors:
        for regressor in regressors:
            model.add_regressor(regressor)

    return model


def compute_metrics(actuals: pd.Series | np.ndarray, predictions: pd.Series | np.ndarray) -> dict:
    actual_values = pd.to_numeric(pd.Series(actuals).reset_index(drop=True), errors='coerce').fillna(0).to_numpy(dtype=float)
    predicted_values = pd.to_numeric(pd.Series(predictions).reset_index(drop=True), errors='coerce').fillna(0).to_numpy(dtype=float)
    errors = actual_values - predicted_values

    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(errors ** 2)))
    non_zero = actual_values != 0
    if np.any(non_zero):
        mape = float(np.mean(np.abs(errors[non_zero]) / np.abs(actual_values[non_zero])) * 100)
    else:
        mape = 0.0

    return {
        'mae': round(mae, 4),
        'rmse': round(rmse, 4),
        'mape': round(mape, 4),
    }


def evaluate_candidate_configurations(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    granularity: str,
    log_transform: bool = True,
) -> tuple[str, list[str], list[dict]]:
    """
    Evaluates three distinct model configurations:
      - MODEL A: Revenue + existing seasonality (baseline)
      - MODEL B: Revenue + existing seasonality + holidays
      - MODEL C: Revenue + existing seasonality + holidays + promotions
    Compares MAE, RMSE, and MAPE and selects the best validated configuration.
    """
    available_cols = set(train_frame.columns)
    available_holidays = [col for col in HOLIDAY_REGRESSORS if col in available_cols]
    available_promos = [col for col in PROMOTION_REGRESSORS if col in available_cols]

    configs = [
        {
            'name': 'Model A (Baseline)',
            'regressors': [],
            'description': 'Revenue + conservative seasonality (no external event regressors)',
        }
    ]

    if available_holidays and (train_frame[available_holidays] != 0).any().any():
        configs.append({
            'name': 'Model B (+ Holidays)',
            'regressors': available_holidays,
            'description': f'Revenue + seasonality + holidays ({", ".join(available_holidays)})',
        })

    if (available_holidays or available_promos) and (
        (train_frame[available_promos] != 0).any().any() if available_promos else False
    ):
        configs.append({
            'name': 'Model C (+ Holidays + Promotions)',
            'regressors': sorted(set(available_holidays + available_promos)),
            'description': 'Revenue + seasonality + holidays + promotions',
        })

    comparison_results = []
    best_config_name = configs[0]['name']
    best_regressors = configs[0]['regressors']
    best_score = float('inf')

    for config in configs:
        model = build_model_with_params(
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10.0,
            holidays_prior_scale=10.0,
            granularity=granularity,
            data_points=len(train_frame),
            regressors=config['regressors'],
        )

        model.fit(train_frame)
        forecast = model.predict(test_frame)

        if log_transform:
            eval_actual = inverse_log1p_series(test_frame['y'])
            eval_pred = inverse_log1p_series(forecast['yhat'])
        else:
            eval_actual = pd.to_numeric(test_frame['y'], errors='coerce').fillna(0)
            eval_pred = pd.to_numeric(forecast['yhat'], errors='coerce').fillna(0)

        metrics = compute_metrics(eval_actual, eval_pred)
        score = metrics['rmse']

        candidate_record = {
            'configuration': config['name'],
            'description': config['description'],
            'regressors': config['regressors'],
            'evaluation': metrics,
            'selected': False,
        }
        comparison_results.append(candidate_record)

        # Select if better RMSE or lower MAPE
        if score < best_score:
            best_score = score
            best_config_name = config['name']
            best_regressors = config['regressors']

    for candidate in comparison_results:
        if candidate['configuration'] == best_config_name:
            candidate['selected'] = True

    print(f'[MODEL EXPERIMENTATION] Completed comparison across {len(comparison_results)} configurations:', file=sys.stderr)
    for c in comparison_results:
        selected_flag = ' [SELECTED]' if c['selected'] else ''
        print(
            f"  {c['configuration']}: MAE={c['evaluation']['mae']:,.2f}, RMSE={c['evaluation']['rmse']:,.2f}, "
            f"MAPE={c['evaluation']['mape']:.2f}%{selected_flag}",
            file=sys.stderr,
        )

    return best_config_name, best_regressors, comparison_results


def train_metric_pipeline(
    metric_name: str,
    series: pd.DataFrame,
    holidays: pd.DataFrame | None,
    output_dir: Path,
    output_prefix: str,
    granularity: str,
    forecast_periods: int,
    test_days: int,
    event_features_df: pd.DataFrame | None = None,
    log_transform: bool = True,
    persist: bool = True,
    allow_negative_y: bool = False,
) -> dict:
    if persist:
        output_dir.mkdir(parents=True, exist_ok=True)

    # Sanity check 1: Validate input series before training
    if not np.all(np.isfinite(series['y'])):
        raise ValueError(f'{metric_name} training data contains non-finite values (NaN or Inf).')
    if not allow_negative_y and (series['y'] < 0).any():
        raise ValueError(f'{metric_name} training data contains negative values.')

    # Merge event features if provided
    if event_features_df is not None and not event_features_df.empty:
        series_with_events = merge_event_features(series, event_features_df, allow_negative_y=allow_negative_y, metric_name=metric_name)
    else:
        series_with_events = series.copy()

    series_for_training = series_with_events.copy()
    if log_transform:
        series_for_training['y'] = pd.to_numeric(series_for_training['y'], errors='coerce').fillna(0).clip(lower=0).apply(math.log1p)

    train_frame, test_frame = split_train_test(series_for_training, test_days=test_days, granularity=granularity)

    # Model Experimentation across Configurations (Model A / B / C)
    selected_name, selected_regressors, model_comparisons = evaluate_candidate_configurations(
        train_frame=train_frame,
        test_frame=test_frame,
        granularity=granularity,
        log_transform=log_transform,
    )

    # Build evaluation model with selected configuration
    eval_model = build_model_with_params(
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10.0,
        holidays_prior_scale=10.0,
        granularity=granularity,
        data_points=len(train_frame),
        regressors=selected_regressors,
    )
    eval_model.fit(train_frame)

    test_forecast = eval_model.predict(test_frame)
    if log_transform:
        test_actual = inverse_log1p_series(test_frame['y'])
        test_predicted = inverse_log1p_series(test_forecast['yhat'])
        test_lower = inverse_log1p_series(test_forecast['yhat_lower'])
        test_upper = inverse_log1p_series(test_forecast['yhat_upper'])
    else:
        test_actual = pd.to_numeric(test_frame['y'], errors='coerce').fillna(0)
        test_predicted = pd.to_numeric(test_forecast['yhat'], errors='coerce').fillna(0)
        test_lower = pd.to_numeric(test_forecast['yhat_lower'], errors='coerce').fillna(0)
        test_upper = pd.to_numeric(test_forecast['yhat_upper'], errors='coerce').fillna(0)

    evaluation = compute_metrics(test_actual, test_predicted)

    # Train Final Production Model on full dataset
    final_model = build_model_with_params(
        changepoint_prior_scale=0.05,
        seasonality_prior_scale=10.0,
        holidays_prior_scale=10.0,
        granularity=granularity,
        data_points=len(series_for_training),
        regressors=selected_regressors,
    )
    final_model.fit(series_for_training)

    # Create Future DataFrame and Attach Future Regressors
    future_freq = 'MS' if granularity == 'monthly' else 'D'
    future = final_model.make_future_dataframe(periods=forecast_periods, freq=future_freq)

    if selected_regressors:
        if event_features_df is not None and not event_features_df.empty:
            future = pd.merge(future, event_features_df, on='ds', how='left')
        for reg in selected_regressors:
            if reg not in future.columns:
                future[reg] = 0.0
            else:
                future[reg] = pd.to_numeric(future[reg], errors='coerce').fillna(0.0).astype(float)

        # Assert all required future regressors exist and are finite
        for reg in selected_regressors:
            if future[reg].isna().any() or not np.all(np.isfinite(future[reg])):
                raise ValueError(f'Future regressor "{reg}" contains missing or non-finite values.')

    forecast = final_model.predict(future)
    required_forecast_columns = {'ds', 'yhat', 'yhat_lower', 'yhat_upper'}
    if forecast is None or forecast.empty:
        raise ValueError(f'{metric_name} forecast generation returned empty data.')
    missing_forecast_columns = required_forecast_columns.difference(forecast.columns)
    if missing_forecast_columns:
        raise ValueError(
            f'{metric_name} forecast is missing required columns: {sorted(missing_forecast_columns)}'
        )

    # Sanity check 2: Validate raw model predictions
    for col in ('yhat', 'yhat_lower', 'yhat_upper'):
        if not np.all(np.isfinite(forecast[col])):
            raise ValueError(f'{metric_name} forecast contains non-finite values in {col}.')
        if log_transform and (forecast[col] > 35.0).any():
            max_log_val = forecast[col].max()
            raise ValueError(f'{metric_name} forecast produced implausibly large log-scale {col} ({max_log_val}).')

    future_forecast = forecast[forecast['ds'] > series['ds'].max()].copy()

    if log_transform:
        forecast = restore_original_scale(forecast, ('yhat', 'yhat_lower', 'yhat_upper'))
        future_forecast = restore_original_scale(future_forecast, ('yhat', 'yhat_lower', 'yhat_upper'))

    # Sanity check 3: Validate restored scale predictions against historical bounds & ceiling
    hist_max = float(series['y'].max())
    hist_min = float(series['y'].min())
    hist_mean = float(series['y'].mean())
    hist_median = float(series['y'].median())

    fcst_max = float(future_forecast['yhat'].max()) if not future_forecast.empty else 0.0
    fcst_min = float(future_forecast['yhat'].min()) if not future_forecast.empty else 0.0
    fcst_mean = float(future_forecast['yhat'].mean()) if not future_forecast.empty else 0.0
    fcst_median = float(future_forecast['yhat'].median()) if not future_forecast.empty else 0.0
    fcst_to_hist_ratio = round(fcst_mean / max(1.0, hist_mean), 2)

    implausible_ceiling = max(hist_max * 10.0, 1e11)
    if (forecast['yhat'] > implausible_ceiling).any():
        bad_rows = forecast[forecast['yhat'] > implausible_ceiling][['ds', 'yhat']].to_dict(orient='records')
        raise ValueError(
            f'{metric_name} forecast contains implausibly large values exceeding ceiling {implausible_ceiling} '
            f'(historical max={hist_max}, mean={hist_mean}, forecast max={fcst_max}): {bad_rows}'
        )
    if (forecast['yhat'] < 0).any():
        raise ValueError(f'{metric_name} forecast contains negative values on restored scale.')

    model_path = output_dir / f'{output_prefix}_model.json'
    metrics_path = output_dir / f'{output_prefix}_metrics.json'
    forecast_path = output_dir / f'{output_prefix}_forecast.csv'
    summary_path = output_dir / f'{output_prefix}_training_summary.json'

    forecast_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    actual_map = dict(zip(series['ds'].dt.strftime('%Y-%m-%d'), series['y']))
    forecast_df['actual'] = forecast_df['ds'].dt.strftime('%Y-%m-%d').map(actual_map)

    if persist:
        model_path.write_text(model_to_json(final_model), encoding='utf-8')
        forecast_df[['ds', 'actual', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv(forecast_path, index=False)

    summary = {
        'target': metric_name,
        'granularity': granularity,
        'target_transform': 'log1p' if log_transform else 'none',
        'data_points': int(len(series)),
        'train_points': int(len(train_frame)),
        'test_points': int(len(test_frame)),
        'history_start': series['ds'].min().date().isoformat(),
        'history_end': series['ds'].max().date().isoformat(),
        'test_start': test_frame['ds'].min().date().isoformat(),
        'test_end': test_frame['ds'].max().date().isoformat(),
        'selected_model_configuration': selected_name,
        'regressors_used': selected_regressors,
        'model_comparison': model_comparisons,
        'evaluation': evaluation,
        'forecast_days': int(forecast_periods),
        'trained_at': datetime.now(timezone.utc).isoformat(),
        'sanity_diagnostics': {
            'hist_min': hist_min,
            'hist_max': hist_max,
            'hist_mean': hist_mean,
            'hist_median': hist_median,
            'fcst_min': fcst_min,
            'fcst_max': fcst_max,
            'fcst_mean': fcst_mean,
            'fcst_median': fcst_median,
            'forecast_to_hist_ratio': fcst_to_hist_ratio,
        },
        'artifacts': {
            'model': str(model_path) if persist else None,
            'metrics': str(metrics_path) if persist else None,
            'forecast': str(forecast_path) if persist else None,
            'summary': str(summary_path) if persist else None,
        },
        'future_rows': int(len(future_forecast)),
        'future_preview': [
            {
                'ds': row['ds'].strftime('%Y-%m-%d') if hasattr(row['ds'], 'strftime') else str(row['ds']),
                'yhat': float(row['yhat']),
                'yhat_lower': float(row['yhat_lower']),
                'yhat_upper': float(row['yhat_upper']),
            }
            for _, row in future_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].head(5).iterrows()
        ],
    }

    metrics_payload = {
        'target': metric_name,
        'granularity': granularity,
        'target_transform': 'log1p' if log_transform else 'none',
        'data_points': int(len(series)),
        'train_points': int(len(train_frame)),
        'test_points': int(len(test_frame)),
        'history_start': series['ds'].min().date().isoformat(),
        'history_end': series['ds'].max().date().isoformat(),
        'test_start': test_frame['ds'].min().date().isoformat(),
        'test_end': test_frame['ds'].max().date().isoformat(),
        'selected_model_configuration': selected_name,
        'regressors_used': selected_regressors,
        'model_comparison': model_comparisons,
        'evaluation': evaluation,
        'sanity_diagnostics': summary['sanity_diagnostics'],
        'trained_at': datetime.now(timezone.utc).isoformat(),
    }

    if persist:
        metrics_path.write_text(json.dumps(metrics_payload, indent=2), encoding='utf-8')
        summary_path.write_text(json.dumps(summary, indent=2), encoding='utf-8')

    return summary


def train_revenue_model(
    workbook_path: Path,
    output_dir: Path,
    sheet_name: str = DEFAULT_SHEET,
    holidays_workbook: Path = DEFAULT_HOLIDAYS_WORKBOOK,
    promotions_workbook: Path = DEFAULT_PROMOTIONS_WORKBOOK,
    test_days: int = 365,
    forecast_days: int | None = None,
    source: str = 'workbook',
    project_id: int | None = None,
    skip_profit: bool = False,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)

    training_source = source.lower().strip()
    store_name = load_project_name_from_database(project_id) if project_id is not None else None

    # Load holidays and promotions
    holidays_raw = load_holidays_frame(holidays_workbook)
    promotions_raw = load_promotions_frame(
        workbook_path=promotions_workbook,
        store_id=project_id,
        store_name=store_name,
    )

    if training_source == 'database':
        if project_id is not None:
            revenue_series = prepare_monthly_series(
                load_metric_frame_from_database('total_revenue', project_id=project_id)
            )
            print(
                f'[STORE FORECAST] store_id={project_id} observations={len(revenue_series)} '
                f'start={revenue_series["ds"].min().date()} end={revenue_series["ds"].max().date()}',
                file=sys.stderr,
                flush=True,
            )
        else:
            revenue_series = prepare_monthly_series(load_revenue_frame(workbook_path, sheet_name))

        revenue_test_days = min(3, max(1, len(revenue_series) // 4)) if len(revenue_series) < 24 else min(12, max(6, len(revenue_series) // 5))
        revenue_forecast_periods = 24 if forecast_days is None else int(forecast_days)

        # Build full date range for event features spanning history and forecast periods
        future_end = revenue_series['ds'].max() + pd.DateOffset(months=revenue_forecast_periods + 2)
        full_event_dates = pd.date_range(revenue_series['ds'].min(), future_end, freq='MS')
        monthly_event_features = build_monthly_event_features(
            date_series=full_event_dates,
            holidays_df=holidays_raw,
            promotions_df=promotions_raw,
        )

        revenue_result = train_metric_pipeline(
            metric_name='revenue',
            series=revenue_series,
            holidays=holidays_raw,
            output_dir=output_dir,
            output_prefix='revenue',
            granularity='monthly',
            forecast_periods=revenue_forecast_periods,
            test_days=revenue_test_days,
            event_features_df=monthly_event_features,
            log_transform=False if project_id is not None else True,
            persist=True,
            allow_negative_y=False,
        )
        revenue_result['scope'] = 'store' if project_id is not None else 'global'
        revenue_result['project_id'] = project_id
        revenue_result['training_mode'] = 'store_only' if project_id is not None else 'global_only'
        if store_name:
            revenue_result['store_name'] = store_name
        revenue_summary_path = Path(revenue_result['artifacts']['summary']) if revenue_result['artifacts']['summary'] else output_dir / 'revenue_training_summary.json'
        revenue_summary_path.write_text(json.dumps(revenue_result, indent=2), encoding='utf-8')

        # Train Cost Model
        cost_result = None
        try:
            if project_id is not None:
                cost_series = prepare_monthly_series(
                    load_cost_frame_from_database(project_id=project_id)
                )
            else:
                cost_series = prepare_monthly_series(load_cost_frame(workbook_path), allow_negative_y=False, metric_name='cost')

            cost_result = train_metric_pipeline(
                metric_name='cost',
                series=cost_series,
                holidays=holidays_raw,
                output_dir=output_dir,
                output_prefix='cost',
                granularity='monthly',
                forecast_periods=revenue_forecast_periods,
                test_days=revenue_test_days,
                event_features_df=monthly_event_features,
                log_transform=False if project_id is not None else True,
                persist=True,
                allow_negative_y=False,
            )
            cost_result['scope'] = 'store' if project_id is not None else 'global'
            cost_result['project_id'] = project_id
            cost_result['training_mode'] = 'store_only' if project_id is not None else 'global_only'
            if store_name:
                cost_result['store_name'] = store_name
            cost_summary_path = Path(cost_result['artifacts']['summary']) if cost_result['artifacts']['summary'] else output_dir / 'cost_training_summary.json'
            cost_summary_path.write_text(json.dumps(cost_result, indent=2), encoding='utf-8')
        except Exception as cost_error:
            print(f'[COST MODEL] Training notice: {cost_error}', file=sys.stderr)
            cost_result = {'error': str(cost_error)}

        # Train Profit Model (Revenue - Cost)
        profit_result = None
        if not skip_profit:
            try:
                if project_id is not None:
                    # Load revenue and cost separately, compute profit as Revenue - Cost
                    rev_monthly = load_metric_frame_from_database('total_revenue', project_id=project_id)
                    cost_monthly = load_cost_frame_from_database(project_id=project_id)
                    
                    # Merge and compute profit
                    merged = pd.merge(rev_monthly, cost_monthly, on='ds', how='inner', suffixes=('_rev', '_cost'))
                    profit_frame = merged[['ds', 'y_rev']].copy()
                    profit_frame['y'] = merged['y_rev'] - merged['y_cost']
                    profit_series = prepare_monthly_series(profit_frame, allow_negative_y=True, metric_name='profit')
                else:
                    # From workbook: compute profit as Revenue - Cost
                    rev_raw = load_revenue_frame(workbook_path, sheet_name)
                    cost_raw = load_cost_frame(workbook_path)
                    
                    merged = pd.merge(rev_raw, cost_raw, on='ds', how='inner', suffixes=('_rev', '_cost'))
                    profit_frame = merged[['ds', 'y_rev']].copy()
                    profit_frame['y'] = merged['y_rev'] - merged['y_cost']
                    profit_series = prepare_monthly_series(profit_frame, allow_negative_y=True, metric_name='profit')

                profit_result = train_metric_pipeline(
                    metric_name='profit',
                    series=profit_series,
                    holidays=holidays_raw,
                    output_dir=output_dir,
                    output_prefix='profit',
                    granularity='monthly',
                    forecast_periods=revenue_forecast_periods,
                    test_days=revenue_test_days,
                    event_features_df=monthly_event_features,
                    log_transform=False if project_id is not None else True,
                    persist=True,
                    allow_negative_y=True,
                )
                profit_result['scope'] = 'store' if project_id is not None else 'global'
                profit_result['project_id'] = project_id
                profit_result['training_mode'] = 'store_only' if project_id is not None else 'global_only'
                if store_name:
                    profit_result['store_name'] = store_name
                profit_summary_path = Path(profit_result['artifacts']['summary']) if profit_result['artifacts']['summary'] else output_dir / 'profit_training_summary.json'
                profit_summary_path.write_text(json.dumps(profit_result, indent=2), encoding='utf-8')
            except Exception as profit_error:
                print(f'[PROFIT MODEL] Training notice: {profit_error}', file=sys.stderr)
                profit_result = {'error': str(profit_error)}

        revenue_result['cost_model'] = cost_result
        revenue_result['profit_model'] = profit_result
        revenue_summary_path = Path(revenue_result['artifacts']['summary']) if revenue_result['artifacts']['summary'] else output_dir / 'revenue_training_summary.json'
        revenue_summary_path.write_text(json.dumps(revenue_result, indent=2), encoding='utf-8')
        return revenue_result

    # Workbook Source (Daily / Monthly)
    raw_frame = load_revenue_frame(workbook_path, sheet_name)
    monthly_series = prepare_monthly_series(raw_frame)
    revenue_forecast_periods = 24 if forecast_days is None else int(forecast_days)
    revenue_test_days = min(3, max(1, len(monthly_series) // 4)) if len(monthly_series) < 24 else min(12, max(6, len(monthly_series) // 5))

    future_end = monthly_series['ds'].max() + pd.DateOffset(months=revenue_forecast_periods + 2)
    full_event_dates = pd.date_range(monthly_series['ds'].min(), future_end, freq='MS')
    monthly_event_features = build_monthly_event_features(
        date_series=full_event_dates,
        holidays_df=holidays_raw,
        promotions_df=promotions_raw,
    )

    final_result = train_metric_pipeline(
        metric_name='revenue',
        series=monthly_series,
        holidays=holidays_raw,
        output_dir=output_dir,
        output_prefix='revenue',
        granularity='monthly',
        forecast_periods=revenue_forecast_periods,
        test_days=revenue_test_days,
        event_features_df=monthly_event_features,
        log_transform=True,
        persist=True,
        allow_negative_y=False,
    )

    # Train Cost Model
    cost_result = None
    try:
        cost_raw = load_cost_frame(workbook_path)
        monthly_cost_series = prepare_monthly_series(cost_raw, allow_negative_y=False, metric_name='cost')
        cost_result = train_metric_pipeline(
            metric_name='cost',
            series=monthly_cost_series,
            holidays=holidays_raw,
            output_dir=output_dir,
            output_prefix='cost',
            granularity='monthly',
            forecast_periods=revenue_forecast_periods,
            test_days=revenue_test_days,
            event_features_df=monthly_event_features,
            log_transform=True,
            persist=True,
            allow_negative_y=False,
        )
    except Exception as cost_error:
        print(f"Cost training notice: {cost_error}")
        cost_result = {'error': str(cost_error)}

    profit_result = None
    try:
        # Compute profit as Revenue - Cost
        cost_raw = load_cost_frame(workbook_path)
        rev_frame = load_revenue_frame(workbook_path, sheet_name)
        
        merged = pd.merge(rev_frame, cost_raw, on='ds', how='inner', suffixes=('_rev', '_cost'))
        profit_frame = merged[['ds', 'y_rev']].copy()
        profit_frame['y'] = merged['y_rev'] - merged['y_cost']
        monthly_profit_series = prepare_monthly_series(profit_frame, allow_negative_y=True, metric_name='profit')
        
        profit_result = train_metric_pipeline(
            metric_name='profit',
            series=monthly_profit_series,
            holidays=holidays_raw,
            output_dir=output_dir,
            output_prefix='profit',
            granularity='monthly',
            forecast_periods=revenue_forecast_periods,
            test_days=revenue_test_days,
            event_features_df=monthly_event_features,
            log_transform=True,
            persist=True,
            allow_negative_y=True,
        )
    except Exception as profit_error:
        print(f"Profit training notice: {profit_error}")
        profit_result = {'error': str(profit_error)}

    final_result['cost_model'] = cost_result
    final_result['profit_model'] = profit_result
    final_result['selected_granularity'] = 'monthly'
    final_result['data_mode'] = 'workbook'

    summary_path = Path(final_result['artifacts']['summary']) if final_result['artifacts']['summary'] else output_dir / 'revenue_training_summary.json'
    summary_path.write_text(json.dumps(final_result, indent=2), encoding='utf-8')
    return final_result


def parse_args():
    parser = argparse.ArgumentParser(description='Train a Prophet revenue model from CIVORA workbook data.')
    parser.add_argument('--workbook', type=Path, default=DEFAULT_WORKBOOK, help='Path to the CIVORA workbook.')
    parser.add_argument('--holidays-workbook', type=Path, default=DEFAULT_HOLIDAYS_WORKBOOK, help='Path to the holidays workbook.')
    parser.add_argument('--promotions-workbook', type=Path, default=DEFAULT_PROMOTIONS_WORKBOOK, help='Path to the promotions workbook.')
    parser.add_argument('--sheet', default=DEFAULT_SHEET, help='Revenue sheet name.')
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT_DIR, help='Directory for model artifacts.')
    parser.add_argument('--test-days', type=int, default=365, help='Number of days to hold out for testing.')
    parser.add_argument('--forecast-days', type=int, default=None, help='Number of future periods to forecast.')
    parser.add_argument('--source', choices=('workbook', 'database'), default='workbook', help='Training source to use.')
    parser.add_argument('--project-id', type=int, default=None, help='Optional store ID for an isolated store-only model.')
    parser.add_argument('--skip-profit', action='store_true', help='Train only the requested revenue model.')
    return parser.parse_args()


def main():
    args = parse_args()
    summary = train_revenue_model(
        workbook_path=args.workbook,
        output_dir=args.output_dir,
        sheet_name=args.sheet,
        holidays_workbook=args.holidays_workbook,
        promotions_workbook=args.promotions_workbook,
        test_days=args.test_days,
        forecast_days=args.forecast_days,
        source=args.source,
        project_id=args.project_id,
        skip_profit=args.skip_profit,
    )
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
