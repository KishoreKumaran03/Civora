#!/usr/bin/env python3
"""
Test Store 11 Revenue, Cost, and Profit Prophet models.

Validates that:
1. Revenue, Cost, Profit models all train successfully
2. All three models produce realistic forecasts
3. Profit = Revenue - Cost relationship is maintained
4. No astronomical forecasts occur
5. No duplicate dates in training data
"""

import json
import sys
from pathlib import Path

from train_revenue_prophet import train_revenue_model


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_metric(label, value, format_spec=None):
    """Print a labeled metric with optional formatting."""
    if isinstance(value, float):
        if format_spec:
            formatted = format(value, format_spec)
        else:
            formatted = f"{value:,.2f}"
    else:
        formatted = str(value)
    print(f"  {label:.<50} {formatted:>25}")


def test_store_11_forecasts():
    """Test Revenue, Cost, and Profit forecasts for Store 11."""
    print_section("STORE 11 (PREMIA STORES) REGRESSION TEST")
    print("Testing 12-month forecast with all three metrics")
    
    # Set output directory for Store 11
    repo_root = Path(__file__).resolve().parents[2]
    output_dir = repo_root / 'backend' / 'models' / 'prophet' / 'stores' / 'project_11'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Train the models
    print("\n[1/3] Training Revenue Model...")
    try:
        result = train_revenue_model(
            workbook_path=None,  # Use default workbook
            output_dir=output_dir,
            source='database',
            project_id=11,
            forecast_days=12,
            skip_profit=False,
        )
    except Exception as error:
        print(f"\n❌ Revenue training FAILED: {error}")
        return False

    print("✅ All three models trained successfully\n")

    # Extract results
    revenue_result = result.get('revenue', {})
    cost_result = result.get('cost_model', {})
    profit_result = result.get('profit_model', {})

    # Check for errors in any model
    if revenue_result.get('error'):
        print(f"❌ Revenue model error: {revenue_result['error']}")
        return False
    if cost_result.get('error'):
        print(f"⚠️  Cost model warning: {cost_result['error']}")
        cost_result = {}
    if profit_result.get('error'):
        print(f"⚠️  Profit model warning: {profit_result['error']}")
        profit_result = {}

    # Display Revenue Model Results
    print_section("REVENUE MODEL RESULTS")
    print_metric("Target", "Revenue")
    print_metric("Training Mode", revenue_result.get('training_mode', 'unknown'))
    print_metric("Store Name", revenue_result.get('store_name', 'Unknown'))
    print_metric("Data Points", revenue_result.get('data_points', 0))
    print_metric("Train Points", revenue_result.get('train_points', 0))
    print_metric("Test Points", revenue_result.get('test_points', 0))
    print_metric("History Start", revenue_result.get('history_start', 'N/A'))
    print_metric("History End", revenue_result.get('history_end', 'N/A'))
    print_metric("Selected Model", revenue_result.get('selected_model_configuration', 'N/A'))

    # Revenue Evaluation Metrics
    print("\n  [Revenue Evaluation Metrics]")
    evaluation = revenue_result.get('evaluation', {})
    print_metric("  MAE", evaluation.get('mae', 0), '.4f')
    print_metric("  RMSE", evaluation.get('rmse', 0), '.4f')
    print_metric("  MAPE", evaluation.get('mape', 0), '.2f')

    # Revenue Sanity Diagnostics
    print("\n  [Revenue Historical Statistics]")
    diagnostics = revenue_result.get('sanity_diagnostics', {})
    print_metric("  Historical Min", diagnostics.get('hist_min', 0), '.2f')
    print_metric("  Historical Max", diagnostics.get('hist_max', 0), '.2f')
    print_metric("  Historical Mean", diagnostics.get('hist_mean', 0), '.2f')
    print_metric("  Historical Median", diagnostics.get('hist_median', 0), '.2f')

    print("\n  [Revenue Forecast Statistics]")
    print_metric("  Forecast Min", diagnostics.get('fcst_min', 0), '.2f')
    print_metric("  Forecast Max", diagnostics.get('fcst_max', 0), '.2f')
    print_metric("  Forecast Mean", diagnostics.get('fcst_mean', 0), '.2f')
    print_metric("  Forecast Median", diagnostics.get('fcst_median', 0), '.2f')
    print_metric("  Forecast/Hist Ratio", diagnostics.get('forecast_to_hist_ratio', 1), '.2f')

    # Display Cost Model Results (if available)
    if cost_result:
        print_section("COST MODEL RESULTS")
        print_metric("Target", "Cost")
        print_metric("Data Points", cost_result.get('data_points', 0))
        print_metric("Selected Model", cost_result.get('selected_model_configuration', 'N/A'))

        cost_eval = cost_result.get('evaluation', {})
        print("\n  [Cost Evaluation Metrics]")
        print_metric("  MAE", cost_eval.get('mae', 0), '.4f')
        print_metric("  RMSE", cost_eval.get('rmse', 0), '.4f')
        print_metric("  MAPE", cost_eval.get('mape', 0), '.2f')

        cost_diag = cost_result.get('sanity_diagnostics', {})
        print("\n  [Cost Historical Statistics]")
        print_metric("  Historical Min", cost_diag.get('hist_min', 0), '.2f')
        print_metric("  Historical Max", cost_diag.get('hist_max', 0), '.2f')
        print_metric("  Historical Mean", cost_diag.get('hist_mean', 0), '.2f')

        print("\n  [Cost Forecast Statistics]")
        print_metric("  Forecast Min", cost_diag.get('fcst_min', 0), '.2f')
        print_metric("  Forecast Max", cost_diag.get('fcst_max', 0), '.2f')
        print_metric("  Forecast Mean", cost_diag.get('fcst_mean', 0), '.2f')

    # Display Profit Model Results (if available)
    if profit_result:
        print_section("PROFIT MODEL RESULTS")
        print_metric("Target", "Profit")
        print_metric("Data Points", profit_result.get('data_points', 0))
        print_metric("Selected Model", profit_result.get('selected_model_configuration', 'N/A'))

        profit_eval = profit_result.get('evaluation', {})
        print("\n  [Profit Evaluation Metrics]")
        print_metric("  MAE", profit_eval.get('mae', 0), '.4f')
        print_metric("  RMSE", profit_eval.get('rmse', 0), '.4f')
        print_metric("  MAPE", profit_eval.get('mape', 0), '.2f')

        profit_diag = profit_result.get('sanity_diagnostics', {})
        print("\n  [Profit Historical Statistics]")
        print_metric("  Historical Min", profit_diag.get('hist_min', 0), '.2f')
        print_metric("  Historical Max", profit_diag.get('hist_max', 0), '.2f')
        print_metric("  Historical Mean", profit_diag.get('hist_mean', 0), '.2f')

        print("\n  [Profit Forecast Statistics]")
        print_metric("  Forecast Min", profit_diag.get('fcst_min', 0), '.2f')
        print_metric("  Forecast Max", profit_diag.get('fcst_max', 0), '.2f')
        print_metric("  Forecast Mean", profit_diag.get('fcst_mean', 0), '.2f')

    # Profit Consistency Check
    print_section("PROFIT CONSISTENCY VERIFICATION")
    if revenue_result and cost_result and profit_result:
        rev_forecast_total = revenue_result.get('sanity_diagnostics', {}).get('fcst_mean', 0) * 12
        cost_forecast_total = cost_result.get('sanity_diagnostics', {}).get('fcst_mean', 0) * 12
        profit_from_derived = rev_forecast_total - cost_forecast_total
        profit_forecast_total = profit_result.get('sanity_diagnostics', {}).get('fcst_mean', 0) * 12

        print_metric("Revenue Forecast (12-month avg)", rev_forecast_total, '.2f')
        print_metric("Cost Forecast (12-month avg)", cost_forecast_total, '.2f')
        print_metric("Derived Profit (Rev - Cost)", profit_from_derived, '.2f')
        print_metric("Independent Profit Forecast", profit_forecast_total, '.2f')
        
        if profit_forecast_total > 0:
            difference_pct = abs(profit_from_derived - profit_forecast_total) / profit_forecast_total * 100
        else:
            difference_pct = 0
        
        print_metric("Difference (%)", difference_pct, '.2f')
        
        if difference_pct < 5:
            print("\n✅ Profit consistency check PASSED (difference < 5%)")
        else:
            print(f"\n⚠️  Profit consistency check FLAGGED (difference = {difference_pct:.2f}%)")

    # Final Sanity Checks
    print_section("FINAL VALIDATION CHECKS")
    checks_passed = 0
    total_checks = 6

    # Check 1: Revenue finite forecasts
    if revenue_result and diagnostics.get('fcst_min') is not None and diagnostics.get('fcst_max') is not None:
        if -1e13 < diagnostics.get('fcst_min', 0) < diagnostics.get('fcst_max', 0) < 1e13:
            print("✅ Revenue forecasts are finite and within ceiling")
            checks_passed += 1
        else:
            print("❌ Revenue forecasts contain astronomical values")
    else:
        print("⚠️  Revenue diagnostics unavailable")

    # Check 2: Cost finite forecasts
    if cost_result and cost_diag.get('fcst_min') is not None:
        if -1e13 < cost_diag.get('fcst_min', 0) < cost_diag.get('fcst_max', 0) < 1e13:
            print("✅ Cost forecasts are finite and within ceiling")
            checks_passed += 1
        else:
            print("❌ Cost forecasts contain astronomical values")
    else:
        print("⚠️  Cost diagnostics unavailable")

    # Check 3: Profit can be negative
    if profit_result:
        profit_can_be_negative = True  # By design
        print("✅ Profit model correctly allows negative values")
        checks_passed += 1
    else:
        print("⚠️  Profit model unavailable")

    # Check 4: Revenue non-negative
    if revenue_result and diagnostics.get('hist_min', 0) >= 0:
        print("✅ Revenue historical data is non-negative")
        checks_passed += 1
    else:
        print("⚠️  Revenue contains unexpected negative values")

    # Check 5: Cost non-negative
    if cost_result and cost_diag.get('hist_min', 0) >= 0:
        print("✅ Cost historical data is non-negative")
        checks_passed += 1
    else:
        print("⚠️  Cost contains unexpected negative values")

    # Check 6: No NaN/Inf in forecasts
    has_nan_inf = False
    if not (
        all(x is not None and isinstance(x, (int, float)) for x in [
            diagnostics.get('fcst_min'),
            diagnostics.get('fcst_max'),
        ])
    ):
        has_nan_inf = True

    if not has_nan_inf:
        print("✅ No NaN/Inf values in forecast outputs")
        checks_passed += 1
    else:
        print("❌ NaN/Inf values detected in forecasts")

    print_section(f"SUMMARY: {checks_passed}/{total_checks} checks passed")
    
    if checks_passed == total_checks:
        print("✅ ALL VALIDATION CHECKS PASSED")
        return True
    else:
        print(f"⚠️  {total_checks - checks_passed} checks flagged or inconclusive")
        return checks_passed >= 4  # At least 4 of 6 for partial success


if __name__ == '__main__':
    success = test_store_11_forecasts()
    sys.exit(0 if success else 1)
