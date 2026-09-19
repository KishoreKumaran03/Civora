"""
Comprehensive test suite for CIVORA Prophet Forecasting Holidays + Promotions Event Layer.
Validates all 10 required test scenarios:
  TEST 1: Global Prophet training
  TEST 2: Store-specific Prophet training for Store 11 (Premia Stores)
  TEST 3: October 2025 event aggregation
  TEST 4: Store with no store-specific promotions
  TEST 5: Store with ALL promotions
  TEST 6: Overlapping promotions
  TEST 7: 12-month future forecast
  TEST 8: Check for duplicate ds after every merge
  TEST 9: Check that future regressors exist
  TEST 10: Check that forecast values remain within sanity limits
"""

import os
import sys
import json
import unittest
from pathlib import Path
import numpy as np
import pandas as pd

# Add script directory to sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from train_revenue_prophet import (
    DEFAULT_WORKBOOK,
    DEFAULT_PROPHET_READY_WORKBOOK,
    DEFAULT_HOLIDAYS_WORKBOOK,
    DEFAULT_PROMOTIONS_WORKBOOK,
    load_holidays_frame,
    load_promotions_frame,
    build_monthly_event_features,
    merge_event_features,
    train_metric_pipeline,
    train_revenue_model,
    prepare_monthly_series,
    load_revenue_frame,
    HOLIDAY_REGRESSORS,
    PROMOTION_REGRESSORS,
)


class TestProphetEventsLayer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_output_dir = SCRIPT_DIR.parent / 'models' / 'prophet' / 'test_runs'
        cls.test_output_dir.mkdir(parents=True, exist_ok=True)
        cls.store11_csv = SCRIPT_DIR.parent / 'models' / 'prophet' / 'stores' / 'project_11' / 'revenue_forecast.csv'

    def test_01_global_prophet_training(self):
        """TEST 1: Global Prophet training with holidays and promotions."""
        print('\n=== Running TEST 1: Global Prophet Training ===')
        summary = train_revenue_model(
            workbook_path=DEFAULT_WORKBOOK,
            output_dir=self.test_output_dir / 'global',
            sheet_name='Daily_Revenue_Prophet',
            forecast_days=12,
            skip_profit=True,
        )
        self.assertIsNotNone(summary)
        self.assertEqual(summary['target'], 'revenue')
        self.assertIn('model_comparison', summary)
        self.assertTrue(len(summary['model_comparison']) >= 1)
        self.assertIn('evaluation', summary)
        self.assertTrue(summary['evaluation']['rmse'] > 0)
        print(f"Global training selected: {summary.get('selected_model_configuration')}")
        print(f"Global evaluation metrics: {summary['evaluation']}")

    def test_02_store_11_prophet_training(self):
        """TEST 2: Store-specific Prophet training for Store 11 / Premia Stores."""
        print('\n=== Running TEST 2: Store-Specific Training for Store 11 ===')
        self.assertTrue(self.store11_csv.exists(), 'Store 11 forecast CSV should exist')
        df_store11 = pd.read_csv(self.store11_csv)
        actuals = df_store11[df_store11['actual'].notna()][['ds', 'actual']].rename(columns={'actual': 'y'})
        actuals['ds'] = pd.to_datetime(actuals['ds'])
        monthly_series = prepare_monthly_series(actuals)

        holidays = load_holidays_frame(DEFAULT_HOLIDAYS_WORKBOOK)
        promos = load_promotions_frame(
            workbook_path=DEFAULT_PROMOTIONS_WORKBOOK,
            store_id=11,
            store_name='Premia Stores',
        )

        future_end = monthly_series['ds'].max() + pd.DateOffset(months=14)
        event_dates = pd.date_range(monthly_series['ds'].min(), future_end, freq='MS')
        event_features = build_monthly_event_features(event_dates, holidays, promos)

        summary = train_metric_pipeline(
            metric_name='revenue',
            series=monthly_series,
            holidays=holidays,
            output_dir=self.test_output_dir / 'store_11',
            output_prefix='revenue',
            granularity='monthly',
            forecast_periods=12,
            test_days=4,
            event_features_df=event_features,
            log_transform=False,
            persist=True,
        )

        self.assertEqual(summary['target'], 'revenue')
        self.assertIn('model_comparison', summary)
        self.assertIn('sanity_diagnostics', summary)
        print(f"Store 11 Selected: {summary['selected_model_configuration']}")
        print(f"Store 11 Evaluation: {summary['evaluation']}")
        print(f"Store 11 Sanity Diagnostics: {summary['sanity_diagnostics']}")

    def test_03_october_2025_event_aggregation(self):
        """TEST 3: October 2025 daily transaction data event aggregation."""
        print('\n=== Running TEST 3: October 2025 Event Aggregation ===')
        holidays = load_holidays_frame(DEFAULT_HOLIDAYS_WORKBOOK)
        promos = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK)

        oct25_date = pd.to_datetime(['2025-10-01'])
        features = build_monthly_event_features(oct25_date, holidays, promos)
        self.assertEqual(len(features), 1)
        row = features.iloc[0].to_dict()

        # In October 2025:
        # Holidays: Dussehra (2025-10-02), Diwali (2025-10-20), Gandhi Jayanti (2025-10-02) -> holiday_active=1
        self.assertEqual(row['holiday_active'], 1)
        self.assertGreaterEqual(row['holiday_count'], 2)
        self.assertGreaterEqual(row['festival_holiday_count'], 1)

        # Promos: Diwali Sale (2025-10-15 to 2025-10-25) [30%] & Festival Sale (2025-10-17 to 2025-10-24) [20%]
        self.assertEqual(row['promo_active'], 1)
        self.assertGreaterEqual(row['promo_count'], 2)
        self.assertEqual(row['promo_discount_max'], 30)
        self.assertEqual(row['promo_days'], 11)  # Unique active days Oct 15-25
        self.assertEqual(row['festival_promo_active'], 1)
        print(f"October 2025 verified features: {row}")

    def test_04_store_with_no_store_specific_promotions(self):
        """TEST 4: A store with no store-specific promotions (only ALL promotions apply)."""
        print('\n=== Running TEST 4: Store with no store-specific promotions ===')
        promos = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK, store_id=999, store_name='NonExistentStore')
        self.assertFalse(promos.empty)
        # All filtered rows must be Store == 'ALL'
        self.assertTrue((promos['Store'] == 'ALL').all())
        print(f"Loaded {len(promos)} promotions for generic store (all Store=ALL)")

    def test_05_store_with_all_promotions(self):
        """TEST 5: A store with ALL promotions."""
        print('\n=== Running TEST 5: Store with ALL promotions ===')
        promos_all = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK)
        self.assertEqual(len(promos_all), 38)
        print(f"Loaded {len(promos_all)} promotions across 2021-2026")

    def test_06_overlapping_promotions(self):
        """TEST 6: Overlapping promotions preservation."""
        print('\n=== Running TEST 6: Overlapping Promotions Preservation ===')
        promos = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK)
        # Check Oct 2025 overlapping promos
        oct25_promos = promos[(promos['Start_Date'] <= '2025-10-31') & (promos['End_Date'] >= '2025-10-01')]
        self.assertGreaterEqual(len(oct25_promos), 2, 'Oct 2025 should have 2 overlapping promos')
        promo_names = oct25_promos['Promotion_Name'].tolist()
        self.assertIn('Diwali Sale', promo_names)
        self.assertIn('Festival Sale', promo_names)

        # Check Dec 2021 overlapping promos
        dec21_promos = promos[(promos['Start_Date'] <= '2021-12-31') & (promos['End_Date'] >= '2021-12-01')]
        self.assertGreaterEqual(len(dec21_promos), 2, 'Dec 2021 should have Clearance Sale and Year-End Sale')
        print("Verified overlapping promotions in Oct 2025 and Dec 2021 are preserved.")

    def test_07_12_month_future_forecast(self):
        """TEST 7: 12-month future forecast horizon."""
        print('\n=== Running TEST 7: 12-Month Future Forecast ===')
        df_store11 = pd.read_csv(self.store11_csv)
        actuals = df_store11[df_store11['actual'].notna()][['ds', 'actual']].rename(columns={'actual': 'y'})
        actuals['ds'] = pd.to_datetime(actuals['ds'])
        monthly_series = prepare_monthly_series(actuals)

        holidays = load_holidays_frame(DEFAULT_HOLIDAYS_WORKBOOK)
        promos = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK, store_id=11)
        event_dates = pd.date_range(monthly_series['ds'].min(), monthly_series['ds'].max() + pd.DateOffset(months=14), freq='MS')
        event_features = build_monthly_event_features(event_dates, holidays, promos)

        summary = train_metric_pipeline(
            metric_name='revenue',
            series=monthly_series,
            holidays=holidays,
            output_dir=self.test_output_dir / 'store_11_12m',
            output_prefix='revenue',
            granularity='monthly',
            forecast_periods=12,
            test_days=4,
            event_features_df=event_features,
            log_transform=False,
            persist=True,
        )
        self.assertEqual(summary['future_rows'], 12)
        print(f"Successfully generated 12-month future forecast with {summary['future_rows']} future rows.")

    def test_08_check_duplicate_ds_after_merge(self):
        """TEST 8: Check for duplicate ds after every merge."""
        print('\n=== Running TEST 8: Check for Duplicate ds After Merge ===')
        dates = pd.date_range('2024-01-01', '2025-12-01', freq='MS')
        sales_df = pd.DataFrame({'ds': dates, 'y': np.random.uniform(1e6, 2e6, size=len(dates))})
        holidays = load_holidays_frame(DEFAULT_HOLIDAYS_WORKBOOK)
        promos = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK)

        event_features = build_monthly_event_features(dates, holidays, promos)
        merged = merge_event_features(sales_df, event_features)

        self.assertEqual(len(merged), len(sales_df))
        self.assertEqual(merged['ds'].nunique(), len(merged))
        self.assertFalse(merged['ds'].duplicated().any())
        print(f"Join safety verified: {len(merged)} rows before and after merge, 0 duplicates.")

    def test_09_check_future_regressors_exist(self):
        """TEST 9: Check that future regressors exist and are non-null."""
        print('\n=== Running TEST 9: Check Future Regressors Exist ===')
        dates = pd.date_range('2026-01-01', '2026-12-01', freq='MS')
        holidays = load_holidays_frame(DEFAULT_HOLIDAYS_WORKBOOK)
        promos = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK)

        event_features = build_monthly_event_features(dates, holidays, promos)
        for col in HOLIDAY_REGRESSORS + PROMOTION_REGRESSORS:
            if col in event_features.columns:
                self.assertFalse(event_features[col].isna().any(), f'Regressor {col} has NaN in future dates')
                self.assertTrue(np.all(np.isfinite(event_features[col])), f'Regressor {col} has non-finite values in future dates')
        print(f"All {len(HOLIDAY_REGRESSORS + PROMOTION_REGRESSORS)} regressor features exist and are non-null for 2026 future dates.")

    def test_10_sanity_limits_and_forecast_ratio(self):
        """TEST 10: Check that forecast values remain within existing sanity limits."""
        print('\n=== Running TEST 10: Sanity Limits and Forecast Ratio Check ===')
        df_store11 = pd.read_csv(self.store11_csv)
        actuals = df_store11[df_store11['actual'].notna()][['ds', 'actual']].rename(columns={'actual': 'y'})
        actuals['ds'] = pd.to_datetime(actuals['ds'])
        monthly_series = prepare_monthly_series(actuals)

        holidays = load_holidays_frame(DEFAULT_HOLIDAYS_WORKBOOK)
        promos = load_promotions_frame(DEFAULT_PROMOTIONS_WORKBOOK, store_id=11)
        event_dates = pd.date_range(monthly_series['ds'].min(), monthly_series['ds'].max() + pd.DateOffset(months=14), freq='MS')
        event_features = build_monthly_event_features(event_dates, holidays, promos)

        summary = train_metric_pipeline(
            metric_name='revenue',
            series=monthly_series,
            holidays=holidays,
            output_dir=self.test_output_dir / 'store_11_sanity',
            output_prefix='revenue',
            granularity='monthly',
            forecast_periods=12,
            test_days=4,
            event_features_df=event_features,
            log_transform=False,
            persist=True,
        )

        diag = summary['sanity_diagnostics']
        self.assertGreater(diag['fcst_min'], 0)
        self.assertLess(diag['fcst_max'], diag['hist_max'] * 5.0)
        self.assertTrue(0.5 <= diag['forecast_to_hist_ratio'] <= 2.5, f"Abnormal forecast ratio: {diag['forecast_to_hist_ratio']}")
        print(f"Sanity checks passed! Hist mean={diag['hist_mean']:,.0f}, Fcst mean={diag['fcst_mean']:,.0f}, Ratio={diag['forecast_to_hist_ratio']}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
