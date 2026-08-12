import { useState, useEffect, useRef } from 'react';
import { useParams, useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { apiRequest } from '../../services/api';
import { COLORS } from '../../constants/colors';
import { MONTH_ORDER } from '../../constants/months';
import {
  ANALYTICS_X_AXIS_OPTIONS,
  ANALYTICS_Y_AXIS_OPTIONS,
  ANALYTICS_TIME_WINDOW_OPTIONS,
  ANALYTICS_PROJECTION_WINDOW_OPTIONS,
} from '../../constants/analyticsConstants';
import { formatInrCompact, formatMetricValue } from '../../utils/formatters';
import { normalizeMapStateName } from '../../utils/mapUtils';
import { DownloadButton } from '../../components/common/DownloadButton';
import { MultiSelectDropdown } from '../../components/common/MultiSelectDropdown';
import { StateMap } from '../../components/charts/StateMap';
import { PieAnalytics } from '../../components/charts/PieAnalytics';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Cell,
  CartesianGrid,
  LineChart,
  Line,
  Legend,
  ComposedChart,
} from 'recharts';

function buildEntriesFromProjectData(projectRows) {
  const normalizedRows = Array.isArray(projectRows) ? projectRows : [];
  const derivedEntries = normalizedRows.flatMap((row, rowIndex) => {
    const monthName = row.month_name || 'January';
    const yearValue = Number(row.year || new Date().getFullYear());
    const recordedAt = `${yearValue}-${String(rowIndex + 1).padStart(2, '0')}-01T09:00:00`;
    const detailedEntries = Array.isArray(row.detailed_entries) ? row.detailed_entries : [];

    if (detailedEntries.length > 0) {
      return detailedEntries.map((detail, detailIndex) => ({
        id: `${row.id || `${monthName}-${yearValue}`}-detail-${detailIndex}`,
        label: String(detail.product || row.top_product || `Product ${detailIndex + 1}`).trim(),
        category: String(detail.category || 'General').trim() || 'General',
        region: normalizeMapStateName(String(detail.region || row.top_region || 'Unknown').trim() || 'Unknown'),
        month: monthName,
        year: yearValue,
        revenue: Number(detail.revenue || 0),
        cost: Number(detail.cost || 0),
        quantity: Number(detail.quantity || 0),
        recordedAt,
      }));
    }

    const regionEntries = Object.entries(row.region_data || {});
    if (regionEntries.length > 0) {
      const categoryEntries = Object.entries(row.category_data || {});
      const topCategory = categoryEntries.sort(([, leftValue], [, rightValue]) => Number(rightValue || 0) - Number(leftValue || 0))[0]?.[0] || 'General';
      const totalRevenue = regionEntries.reduce((sum, [, value]) => sum + Number(value || 0), 0) || 1;
      const totalCost = Number(row.total_cost || 0);
      const totalQuantity = Number(row.total_quantity || 0);

      return regionEntries.map(([regionName, regionRevenue], regionIndex) => {
        const revenue = Number(regionRevenue || 0);
        const share = revenue / totalRevenue;
        return {
          id: `${row.id || `${monthName}-${yearValue}`}-region-${regionIndex}`,
          label: String(row.top_product || 'General Product').trim(),
          category: topCategory,
          region: normalizeMapStateName(String(regionName || row.top_region || 'Unknown').trim() || 'Unknown'),
          month: monthName,
          year: yearValue,
          revenue,
          cost: Math.round(totalCost * share),
          quantity: Math.max(1, Math.round(totalQuantity * share)),
          recordedAt,
        };
      });
    }

    const categoryEntries = Object.entries(row.category_data || {});
    if (categoryEntries.length > 0) {
      const totalCategoryRevenue = categoryEntries.reduce((sum, [, value]) => sum + Number(value || 0), 0) || 1;
      return categoryEntries.map(([categoryName, categoryRevenue], categoryIndex) => {
        const revenue = Number(categoryRevenue || 0);
        const revenueShare = revenue / totalCategoryRevenue;
        return {
          id: `${row.id || `${monthName}-${yearValue}`}-category-${categoryIndex}`,
          label: String(row.top_product || categoryName || `Product ${categoryIndex + 1}`).trim(),
          category: String(categoryName || 'General').trim() || 'General',
          region: normalizeMapStateName(String(row.top_region || 'Unknown').trim() || 'Unknown'),
          month: monthName,
          year: yearValue,
          revenue,
          cost: Math.round(Number(row.total_cost || 0) * revenueShare),
          quantity: Math.max(1, Math.round(Number(row.total_quantity || 0) * revenueShare)),
          recordedAt,
        };
      });
    }

    return [{
      id: `${row.id || `${monthName}-${yearValue}`}-summary`,
      label: String(row.top_product || 'General Product').trim(),
      category: 'General',
      region: normalizeMapStateName(String(row.top_region || 'Unknown').trim() || 'Unknown'),
      month: monthName,
      year: yearValue,
      revenue: Number(row.total_revenue || 0),
      cost: Number(row.total_cost || 0),
      quantity: Number(row.total_quantity || 0),
      recordedAt,
    }];
  });

  return derivedEntries;
}

function getAnalyticsDimension(entry, dimension) {
  if (dimension === 'month') return entry.month || new Date(entry.recordedAt).toLocaleString('en-US', { month: 'long' });
  if (dimension === 'year') return String(entry.year || new Date(entry.recordedAt).getFullYear());
  if (dimension === 'category') return entry.category;
  if (dimension === 'region') return entry.region;
  return entry.label;
}

function getAnalyticsMetric(entry, metric) {
  if (metric === 'cost') return Number(entry.cost || 0);
  if (metric === 'quantity') return Number(entry.quantity || 0);
  if (metric === 'profit') return Number(entry.revenue || 0) - Number(entry.cost || 0);
  return Number(entry.revenue || 0);
}

function aggregateEntriesForAxis(entries, xAxis, yAxis, options = {}) {
  const { averageMonthAcrossYears = false } = options;
  let groupedEntries = {};

  if (xAxis === 'month' && averageMonthAcrossYears) {
    const groupedByMonthYear = entries.reduce((groups, entry) => {
      const monthValue = getAnalyticsDimension(entry, 'month');
      const yearValue = getAnalyticsDimension(entry, 'year');
      const key = `${monthValue}-${yearValue}`;

      if (!groups[key]) {
        groups[key] = {
          month: monthValue,
          year: yearValue,
          value: 0,
        };
      }

      groups[key].value += getAnalyticsMetric(entry, yAxis);
      return groups;
    }, {});

    groupedEntries = Object.values(groupedByMonthYear).reduce((months, row) => {
      if (!months[row.month]) {
        months[row.month] = {
          total: 0,
          count: 0,
        };
      }

      months[row.month].total += row.value;
      months[row.month].count += 1;
      return months;
    }, {});
  } else {
    groupedEntries = entries.reduce((groups, entry) => {
      const dimensionValue = getAnalyticsDimension(entry, xAxis);
      groups[dimensionValue] = (groups[dimensionValue] || 0) + getAnalyticsMetric(entry, yAxis);
      return groups;
    }, {});
  }

  return Object.entries(groupedEntries)
    .map(([name, value], index) => ({
      name,
      value: xAxis === 'month' && averageMonthAcrossYears
        ? (Number(value.total || 0) / Math.max(1, Number(value.count || 0)))
        : value,
      fill: COLORS[index % COLORS.length],
    }))
    .sort((leftEntry, rightEntry) => {
      if (xAxis === 'month') {
        return MONTH_ORDER.indexOf(leftEntry.name) - MONTH_ORDER.indexOf(rightEntry.name);
      }
      if (xAxis === 'year') {
        return Number(leftEntry.name) - Number(rightEntry.name);
      }
      return leftEntry.name.localeCompare(rightEntry.name);
    });
}

function buildHistogramData(entries, yAxis) {
  const metricValues = entries.map((entry) => getAnalyticsMetric(entry, yAxis)).filter((value) => Number.isFinite(value));
  const maxValue = metricValues.length ? Math.max(...metricValues) : 0;
  const bucketSize = maxValue > 0 ? Math.max(1, Math.ceil(maxValue / 5)) : 1;

  return Array.from({ length: 5 }, (_, index) => {
    const min = index * bucketSize;
    const max = index === 4 ? Number.POSITIVE_INFINITY : (index + 1) * bucketSize;
    const count = metricValues.filter((value) => value >= min && value < max).length;

    return {
      name: max === Number.POSITIVE_INFINITY ? `${formatInrCompact(min)}+` : `${formatInrCompact(min)}-${formatInrCompact(max)}`,
      count,
    };
  });
}

export function AdvancedAnalyticsBoard() {
  const monthlyTrendChartRef = useRef(null);
  const topProductsChartRef = useRef(null);
  const pieChartRef1 = useRef(null);
  const histogramChartRef = useRef(null);
  const projectionChartRef = useRef(null);
  const stateMapPanelRef = useRef(null);
  const topRegionsPanelRef = useRef(null);
  const overviewPanelRef = useRef(null);
  const { projectId } = useParams();
  const location = useLocation();
  const { token } = useAuth();
  const [entries, setEntries] = useState([]);
  const [projectRows, setProjectRows] = useState([]);
  const [selectedProjectName, setSelectedProjectName] = useState(location.state?.projectName || 'Advanced Analytics');
  const [isProjectLoading, setIsProjectLoading] = useState(false);
  const [selectedXAxis, setSelectedXAxis] = useState('month');
  const [selectedYAxes, setSelectedYAxes] = useState(['revenue']);
  const [selectedCategoryFilter, setSelectedCategoryFilter] = useState('all');
  const [selectedYearFilter, setSelectedYearFilter] = useState('all');
  const [selectedTimeWindow, setSelectedTimeWindow] = useState('all');
  const [projectionWindow, setProjectionWindow] = useState('3');
  const [compareProducts, setCompareProducts] = useState([]);
  const [compareMonths, setCompareMonths] = useState([]);
  const [compareRegions, setCompareRegions] = useState([]);
  const [formState, setFormState] = useState({
    label: '',
    category: '',
    region: '',
    month: '',
    year: '',
    revenue: '',
    cost: '',
    quantity: '',
  });

  useEffect(() => {
    if (!projectId) {
      setSelectedProjectName(location.state?.projectName || 'Advanced Analytics');
      setEntries([]);
      setProjectRows([]);
      return;
    }

    const fetchProjectAnalytics = async () => {
      try {
        setIsProjectLoading(true);
        const response = await apiRequest({
          method: 'get',
          url: `/api/dashboard/${projectId}`,
          headers: { Authorization: `Bearer ${token}` },
        });

        const rows = Array.isArray(response.data) ? response.data : [];
        const rowYears = [...new Set(rows.map((row) => String(row.year || '')).filter(Boolean))]
          .sort((left, right) => Number(left) - Number(right));
        setProjectRows(rows);
        setEntries(buildEntriesFromProjectData(rows));
        if (rowYears.length > 0) {
          setSelectedYearFilter(rowYears[rowYears.length - 1]);
        }
        setSelectedProjectName(location.state?.projectName || `Project ${projectId}`);
      } catch (error) {
        console.error('Error fetching advanced analytics project data:', error);
        setProjectRows([]);
        setEntries([]);
        setSelectedYearFilter('all');
        setSelectedProjectName(location.state?.projectName || `Project ${projectId}`);
      } finally {
        setIsProjectLoading(false);
      }
    };

    fetchProjectAnalytics();
  }, [location.state, projectId, token]);

  const availableCategoryOptions = [...new Set(entries.map((entry) => entry.category).filter(Boolean))].sort((left, right) => left.localeCompare(right));
  const availableRegionOptions = [...new Set(entries.map((entry) => entry.region).filter(Boolean))].sort((left, right) => left.localeCompare(right));
  const availableProductOptions = [...new Set(entries.map((entry) => entry.label).filter(Boolean))].sort((left, right) => left.localeCompare(right));
  const availableYearOptions = [...new Set(entries.map((entry) => String(entry.year)).filter(Boolean))]
    .sort((left, right) => Number(left) - Number(right));
  const availableMonthOptions = [...new Set(entries.map((entry) => entry.month).filter(Boolean))]
    .sort((left, right) => {
      const leftIndex = MONTH_ORDER.indexOf(left);
      const rightIndex = MONTH_ORDER.indexOf(right);
      if (leftIndex >= 0 && rightIndex >= 0) return leftIndex - rightIndex;
      return left.localeCompare(right);
    });

  const getEntryPeriodKey = (entry) => {
    const yearValue = Number(entry.year || new Date(entry.recordedAt).getFullYear());
    const monthIndex = MONTH_ORDER.indexOf(entry.month);
    const safeMonthIndex = monthIndex >= 0 ? monthIndex : 0;
    return yearValue * 12 + safeMonthIndex;
  };

  const filteredEntries = entries.filter((entry) => {
    const matchesCategory = selectedCategoryFilter === 'all' || entry.category === selectedCategoryFilter;
    const matchesYear = selectedYearFilter === 'all' || String(entry.year) === selectedYearFilter;
    const matchesCompareProduct = compareProducts.length === 0 || compareProducts.includes(entry.label);
    const matchesCompareMonth = compareMonths.length === 0 || compareMonths.includes(entry.month);
    const matchesCompareRegion = compareRegions.length === 0 || compareRegions.includes(entry.region);
    return matchesCategory && matchesYear && matchesCompareProduct && matchesCompareMonth && matchesCompareRegion;
  });

  const windowFilteredEntries = (() => {
    if (selectedTimeWindow === 'all' || filteredEntries.length === 0) {
      return filteredEntries;
    }

    if (selectedTimeWindow === 'year') {
      const latestYear = Math.max(...filteredEntries.map((entry) => Number(entry.year || 0)));
      return filteredEntries.filter((entry) => Number(entry.year || 0) === latestYear);
    }

    const monthsToKeep = Number(selectedTimeWindow);
    if (!Number.isFinite(monthsToKeep) || monthsToKeep <= 0) {
      return filteredEntries;
    }

    const uniquePeriods = [...new Set(filteredEntries.map((entry) => getEntryPeriodKey(entry)))].sort((left, right) => left - right);
    const recentPeriods = new Set(uniquePeriods.slice(-monthsToKeep));
    return filteredEntries.filter((entry) => recentPeriods.has(getEntryPeriodKey(entry)));
  })();

  const sortedEntries = [...windowFilteredEntries].sort((leftEntry, rightEntry) => new Date(leftEntry.recordedAt) - new Date(rightEntry.recordedAt));
  const selectedYAxis = selectedYAxes[0] || 'revenue';
  const totalRevenue = sortedEntries.reduce((sum, entry) => sum + entry.revenue, 0);
  const totalCost = sortedEntries.reduce((sum, entry) => sum + entry.cost, 0);
  const totalProfit = totalRevenue - totalCost;
  const avgRevenue = sortedEntries.length ? Math.round(totalRevenue / sortedEntries.length) : 0;
  const selectedYAxisLabel = ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === selectedYAxis)?.label || 'Revenue';
  const selectedXAxisLabel = ANALYTICS_X_AXIS_OPTIONS.find((option) => option.value === selectedXAxis)?.label || 'Month';
  const shouldAverageAcrossYears = selectedYearFilter === 'all';
  const aggregatedAxisData = aggregateEntriesForAxis(sortedEntries, selectedXAxis, selectedYAxis, {
    averageMonthAcrossYears: shouldAverageAcrossYears,
  });
  const projectionMetricKeys = selectedYAxes.length > 0 ? selectedYAxes : ['revenue'];
  const projectionMetricLabels = projectionMetricKeys
    .map((metricKey) => ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === metricKey)?.label || metricKey);
  const productAxisData = aggregateEntriesForAxis(sortedEntries, 'label', selectedYAxis);
  const categoryAxisData = aggregateEntriesForAxis(sortedEntries, 'category', selectedYAxis);
  const pieData = categoryAxisData.map((entry) => ({ name: entry.name, value: entry.value, color: entry.fill }));
  const histogramRanges = buildHistogramData(sortedEntries, selectedYAxis);
  const selectedMetricTotal = sortedEntries.reduce((sum, entry) => sum + getAnalyticsMetric(entry, selectedYAxis), 0);
  const projectionTrendRows = (() => {
    const grouped = sortedEntries.reduce((accumulator, entry) => {
      const monthName = entry.month || 'January';
      const yearValue = Number(entry.year || new Date(entry.recordedAt).getFullYear());
      const monthIndex = MONTH_ORDER.indexOf(monthName);
      const safeMonthIndex = monthIndex >= 0 ? monthIndex : 0;
      const key = `${yearValue}-${safeMonthIndex}`;

      if (!accumulator[key]) {
        accumulator[key] = {
          year: yearValue,
          monthIndex: safeMonthIndex,
          monthName: MONTH_ORDER[safeMonthIndex],
        };
        projectionMetricKeys.forEach((metricKey) => {
          accumulator[key][metricKey] = 0;
        });
      }

      projectionMetricKeys.forEach((metricKey) => {
        accumulator[key][metricKey] += getAnalyticsMetric(entry, metricKey);
      });
      return accumulator;
    }, {});

    const actualBuckets = Object.values(grouped).sort((leftEntry, rightEntry) => {
      if (leftEntry.year !== rightEntry.year) return leftEntry.year - rightEntry.year;
      return leftEntry.monthIndex - rightEntry.monthIndex;
    });

    const actualRows = actualBuckets.map((entry) => {
      const row = {
        name: `${entry.monthName.slice(0, 3)} ${String(entry.year).slice(-2)}`,
      };
      projectionMetricKeys.forEach((metricKey) => {
        row[`${metricKey}_actual`] = Number(entry[metricKey] || 0);
        row[`${metricKey}_projected`] = null;
      });
      return row;
    });

    if (actualRows.length === 0) {
      return [];
    }

    const lastBucket = actualBuckets[actualBuckets.length - 1];
    let monthIndex = lastBucket.monthIndex;
    let yearValue = lastBucket.year;
    const futureRows = [];

    const projectedMonths = Number(projectionWindow) || 3;
    for (let index = 1; index <= projectedMonths; index += 1) {
      monthIndex += 1;
      if (monthIndex > 11) {
        monthIndex = 0;
        yearValue += 1;
      }

      futureRows.push({
        name: `${MONTH_ORDER[monthIndex].slice(0, 3)} ${String(yearValue).slice(-2)}`,
      });
    }

    projectionMetricKeys.forEach((metricKey) => {
      const lastActual = Number(actualRows[actualRows.length - 1][`${metricKey}_actual`] || 0);
      actualRows[actualRows.length - 1][`${metricKey}_projected`] = lastActual;
      const previousActual = actualRows.length > 1
        ? Number(actualRows[actualRows.length - 2][`${metricKey}_actual`] || 0)
        : 0;
      const growthRate = previousActual > 0 ? (lastActual - previousActual) / previousActual : 0.08;
      let forecastBase = lastActual;

      futureRows.forEach((row) => {
        forecastBase = Math.max(0, Math.round(forecastBase * (1 + growthRate)));
        row[`${metricKey}_actual`] = null;
        row[`${metricKey}_projected`] = forecastBase;
      });
    });

    return [...actualRows, ...futureRows];
  })();
  const realtimeFeed = [...sortedEntries]
    .sort((leftEntry, rightEntry) => new Date(rightEntry.recordedAt) - new Date(leftEntry.recordedAt))
    .slice(0, 5);

  const stateMapData = sortedEntries.reduce((accumulator, entry) => {
    const stateName = normalizeMapStateName(entry.region || 'Unknown');
    accumulator[stateName] = (accumulator[stateName] || 0) + Number(entry.revenue || 0);
    return accumulator;
  }, {});

  const fallbackMonthlyTrendRows = Object.values(sortedEntries.reduce((accumulator, entry) => {
    const monthName = entry.month || 'January';
    const yearValue = Number(entry.year || new Date(entry.recordedAt).getFullYear());
    const monthIndex = MONTH_ORDER.indexOf(monthName);
    const safeMonthIndex = monthIndex >= 0 ? monthIndex : 0;
    const key = `${yearValue}-${safeMonthIndex}`;
    if (!accumulator[key]) {
      accumulator[key] = {
        year: yearValue,
        monthIndex: safeMonthIndex,
        month_name: MONTH_ORDER[safeMonthIndex],
        total_revenue: 0,
        total_cost: 0,
      };
    }
    accumulator[key].total_revenue += Number(entry.revenue || 0);
    accumulator[key].total_cost += Number(entry.cost || 0);
    return accumulator;
  }, {}))
    .sort((leftEntry, rightEntry) => {
      if (leftEntry.year !== rightEntry.year) return leftEntry.year - rightEntry.year;
      return leftEntry.monthIndex - rightEntry.monthIndex;
    });

  const filteredProjectRows = (projectRows.length > 0 ? projectRows : fallbackMonthlyTrendRows)
    .filter((row) => selectedYearFilter === 'all' || String(row.year) === selectedYearFilter);

  const monthlyTrendRows = (shouldAverageAcrossYears
    ? Object.values(filteredProjectRows.reduce((accumulator, row) => {
      const monthName = row.month_name || row.name || 'January';
      const monthIndex = MONTH_ORDER.indexOf(monthName);
      const safeMonthIndex = monthIndex >= 0 ? monthIndex : 0;

      if (!accumulator[monthName]) {
        accumulator[monthName] = {
          name: String(monthName).slice(0, 3),
          revenueTotal: 0,
          profitTotal: 0,
          count: 0,
          monthIndex: safeMonthIndex,
          metricLabel: 'Revenue',
        };
      }

      accumulator[monthName].revenueTotal += Number(row.total_revenue || row.value || 0);
      accumulator[monthName].profitTotal += Number(row.net_revenue ?? (Number(row.total_revenue || row.value || 0) - Number(row.total_cost || 0)));
      accumulator[monthName].count += 1;
      return accumulator;
    }, {})).map((row) => ({
      name: row.name,
      revenue: row.revenueTotal / Math.max(1, row.count),
      profit: row.profitTotal / Math.max(1, row.count),
      metricLabel: row.metricLabel,
      monthIndex: row.monthIndex,
      year: 0,
    }))
    : filteredProjectRows
      .map((row, index) => ({
        name: row.month_name
          ? String(row.month_name).slice(0, 3)
          : (row.name || `Point ${index + 1}`),
        revenue: Number(row.total_revenue || row.value || 0),
        profit: Number(row.net_revenue ?? (Number(row.total_revenue || row.value || 0) - Number(row.total_cost || 0))),
        metricLabel: 'Revenue',
        monthIndex: MONTH_ORDER.indexOf(row.month_name || ''),
        year: Number(row.year || 0),
      })))
    .sort((leftEntry, rightEntry) => {
      if (leftEntry.year !== rightEntry.year) return leftEntry.year - rightEntry.year;
      return leftEntry.monthIndex - rightEntry.monthIndex;
    });

  const previousPeriodRevenue = monthlyTrendRows.length > 1 ? monthlyTrendRows[monthlyTrendRows.length - 2].revenue : 0;
  const latestPeriodRevenue = monthlyTrendRows.length > 0 ? monthlyTrendRows[monthlyTrendRows.length - 1].revenue : totalRevenue;
  const salesGrowth = previousPeriodRevenue > 0 ? ((latestPeriodRevenue - previousPeriodRevenue) / previousPeriodRevenue) * 100 : 0;
  const activeRegions = availableRegionOptions.length;
  const activeCategories = availableCategoryOptions.length;

  const categoryOverviewRows = categoryAxisData
    .map((entry, index) => ({
      name: entry.name,
      value: entry.value,
      color: COLORS[index % COLORS.length],
      share: selectedMetricTotal > 0 ? (Number(entry.value || 0) / selectedMetricTotal) * 100 : 0,
    }))
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value);

  const topProductsChartData = [...productAxisData]
    .filter((entry) => String(entry.name || '').trim().length > 0)
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value)
    .map((entry) => ({ ...entry, fill: '#38bdf8' }));

  const topRegionRows = Object.entries(stateMapData)
    .map(([name, value]) => ({ name, value: Number(value || 0) }))
    .sort((leftEntry, rightEntry) => rightEntry.value - leftEntry.value)
    .slice(0, 4);

  const monthBasedRows = (monthlyTrendRows.length > 0
    ? monthlyTrendRows
      .map((entry) => ({
        month: entry.name,
        sales: Number(entry.revenue || 0),
        profit: Number(entry.profit || 0),
        margin: Number(entry.revenue || 0) > 0 ? (Number(entry.profit || 0) / Number(entry.revenue || 0)) * 100 : 0,
      }))
      .sort((leftEntry, rightEntry) => rightEntry.sales - leftEntry.sales)
      .slice(0, 5)
    : [
      {
        month: 'Current',
        sales: totalRevenue,
        profit: totalProfit,
        margin: totalRevenue > 0 ? (totalProfit / totalRevenue) * 100 : 0,
      },
    ]);

  const overviewRows = [
    ...monthBasedRows,
    ...Array.from({ length: Math.max(0, 5 - monthBasedRows.length) }, (_, index) => ({
      month: `-`,
      sales: null,
      profit: null,
      margin: null,
      id: `placeholder-${index + 1}`,
    })),
  ];

  const handleFormChange = (field, value) => {
    setFormState((currentState) => ({ ...currentState, [field]: value }));
  };

  const handleAddEntry = (event) => {
    event.preventDefault();

    const revenue = Number(formState.revenue);
    const cost = Number(formState.cost);
    const quantity = Number(formState.quantity);

    const yearValue = Number(formState.year);
    if (!formState.label.trim() || !formState.category.trim() || !formState.region.trim() || !formState.month || !Number.isFinite(yearValue) || revenue <= 0 || cost < 0 || quantity <= 0) {
      alert('Enter product, category, region, month, year, and valid numeric values before adding a dataset row.');
      return;
    }

    setEntries((currentEntries) => [
      ...currentEntries,
      {
        id: Date.now(),
        label: formState.label.trim(),
        category: formState.category.trim(),
        region: formState.region.trim(),
        month: formState.month,
        year: yearValue,
        revenue,
        cost,
        quantity,
        recordedAt: `${yearValue}-${String(Math.max(1, MONTH_ORDER.indexOf(formState.month) + 1)).padStart(2, '0')}-01T09:00:00`,
      },
    ]);

    setFormState({
      label: '',
      category: formState.category,
      region: formState.region,
      month: formState.month,
      year: formState.year,
      revenue: '',
      cost: '',
      quantity: '',
    });
  };

  const handleClearImportedData = async () => {
    if (!projectId) {
      alert('Open a project before clearing imported data.');
      return;
    }

    const confirmed = window.confirm(
      'Clear imported analytics data for only this current project? This keeps the project/entity itself but removes its uploaded month data.'
    );

    if (!confirmed) {
      return;
    }

    try {
      const response = await apiRequest({
        method: 'delete',
        url: `/api/dashboard/${projectId}`,
        headers: { Authorization: `Bearer ${token}` },
      });
      setEntries([]);
      setProjectRows([]);
      alert(response.data?.message || 'Imported data cleared successfully.');
    } catch (error) {
      alert(`Failed to clear imported data: ${error.response?.data?.error || error.message}`);
    }
  };

  return (
    <div className="mx-auto max-w-[1650px] space-y-8 p-8">
      <div className="rounded-[2.5rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900">
        <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <div className="text-[10px] font-black uppercase tracking-[0.3em] text-sky-500">Advanced Analytics</div>
            <h1 className="mt-3 text-4xl font-black tracking-tighter text-slate-900 dark:text-white">Store Intelligence Board</h1>
            <p className="mt-3 max-w-3xl text-sm font-medium text-slate-500 dark:text-slate-300">
              The layout matches your requested analytics board while staying connected to backend project data, axis controls, and the live dataset editor.
            </p>
            <div className="mt-4 inline-flex items-center gap-3 rounded-full border border-sky-100 bg-sky-50 px-4 py-2 text-[10px] font-black uppercase tracking-[0.2em] text-sky-600 dark:border-sky-900/40 dark:bg-sky-950/30 dark:text-sky-300">
              <span className="material-symbols-outlined text-sm">storefront</span>
              <span>{selectedProjectName}</span>
              {projectId && <span className="text-slate-400">ID {projectId}</span>}
            </div>
            {availableYearOptions.length > 0 && (
              <div className="mt-4 flex flex-wrap gap-2">
                {availableYearOptions.map((year) => {
                  const isActive = selectedYearFilter === year;
                  return (
                    <button
                      key={year}
                      type="button"
                      onClick={() => setSelectedYearFilter(year)}
                      className={`rounded-full px-4 py-2 text-[10px] font-black uppercase tracking-[0.2em] transition-all ${isActive
                        ? 'bg-slate-900 text-white shadow-lg shadow-slate-900/15 dark:bg-white dark:text-slate-900'
                        : 'border border-slate-200 bg-white text-slate-500 hover:border-slate-300 hover:text-slate-900 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300 dark:hover:border-slate-600 dark:hover:text-white'
                        }`}
                    >
                      {year}
                    </button>
                  );
                })}
              </div>
            )}
          </div>
          <div className="grid grid-cols-2 gap-4 2xl:grid-cols-3">
            <AnalyticsStat title="Total Sales" value={formatInrCompact(totalRevenue)} sub={`${activeRegions} live regions`} />
            <AnalyticsStat title="Profit" value={formatInrCompact(totalProfit)} sub={`${totalRevenue > 0 ? Math.round((totalProfit / totalRevenue) * 100) : 0}% margin`} />
            <AnalyticsStat title="Sales Growth" value={`${salesGrowth >= 0 ? '+' : ''}${salesGrowth.toFixed(1)}%`} sub={monthlyTrendRows.length > 1 ? 'vs previous period' : 'baseline ready'} />
            <AnalyticsStat title="Rows" value={sortedEntries.length} sub="retrieved rows" />
            <AnalyticsStat title="Categories" value={activeCategories} sub="dynamic category mix" />
            <AnalyticsStat title="Avg Ticket" value={formatInrCompact(avgRevenue)} sub="average revenue per row/transaction entry" />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-8 xl:grid-cols-[0.95fr_1.45fr]">
        <AnalyticsPanel title="Sales By Region" subtitle="Dynamic India heat map from uploaded state data" downloadRef={stateMapPanelRef} downloadFilename="sales-by-region-heatmap" panelRef={stateMapPanelRef}>
          <div className="h-[420px]">
            <StateMap stateData={stateMapData} hasData={Object.keys(stateMapData).length > 0} mapScale={0.93} />
          </div>
        </AnalyticsPanel>

        <div className="self-stretch">
          <AnalyticsPanel title="Revenue & Profit Trend" subtitle="One bar graph plus one line graph, driven by backend totals" className="h-full" downloadRef={monthlyTrendChartRef} downloadFilename="monthly-trend-chart">
            <div className="relative h-[420px]">
              <div ref={monthlyTrendChartRef} className="w-full h-full">
                <ResponsiveContainer width="100%" height="100%">
                  <ComposedChart data={monthlyTrendRows}>
                    <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                    <XAxis
                      dataKey="name"
                      axisLine={false}
                      tickLine={false}
                      interval={0}
                      minTickGap={0}
                      tick={{ fontSize: 11, fontWeight: 700 }}
                    />
                    <YAxis yAxisId="left" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                    <YAxis yAxisId="right" orientation="right" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                    <Tooltip
                      formatter={(value, name, item) => {
                        const metricName = item?.dataKey === 'profit' ? 'Profit' : 'Revenue';
                        return [formatInrCompact(value), metricName];
                      }}
                    />
                    <Legend />
                    <Bar yAxisId="left" dataKey="revenue" name="Revenue" fill="#38bdf8" radius={[10, 10, 0, 0]} />
                    <Line yAxisId="right" type="monotone" dataKey="profit" name="Profit" stroke="#f59e0b" strokeWidth={3} dot={{ r: 4 }} />
                  </ComposedChart>
                </ResponsiveContainer>
              </div>
            </div>
          </AnalyticsPanel>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-8 xl:grid-cols-2 2xl:grid-cols-[1fr_1fr_0.95fr] items-start">
        <div className="space-y-8">
          <AnalyticsPanel title="Bar Chart" subtitle={`${selectedYAxisLabel} by Product`} className="min-h-[28rem]" downloadRef={topProductsChartRef} downloadFilename="top-products-chart">
            <div className="max-h-[24rem] overflow-y-auto pr-1">
              <div className="relative" style={{ height: `${Math.max(320, topProductsChartData.length * 34)}px`, minHeight: '320px' }}>
                <div ref={topProductsChartRef} className="w-full h-full">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={topProductsChartData} layout="vertical" margin={{ left: 10, right: 10 }}>
                      <CartesianGrid horizontal={false} strokeDasharray="3 3" opacity={0.12} />
                      <XAxis type="number" axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                      <YAxis type="category" dataKey="name" interval={0} width={130} axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} tickFormatter={(label) => String(label || 'Unknown')} />
                      <Tooltip
                        formatter={(value) => [formatMetricValue(selectedYAxis, value), selectedYAxisLabel]}
                        labelFormatter={(label) => `Product: ${label}`}
                      />
                      <Bar dataKey="value" name={selectedYAxisLabel} radius={[0, 10, 10, 0]}>
                        {topProductsChartData.map((entry) => <Cell key={entry.name} fill={entry.fill} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          </AnalyticsPanel>

          <AnalyticsPanel title="Top Regions" subtitle={`Highest ${selectedYAxisLabel.toLowerCase()} states from active rows`} className="min-h-[24rem]" downloadRef={topRegionsPanelRef} downloadFilename="highest-revenue-states" panelRef={topRegionsPanelRef}>
            <div className="grid grid-cols-1 gap-3">
              {(topRegionRows.length > 0 ? topRegionRows : [{ name: 'Awaiting data', value: 0 }]).map((entry, index) => (
                <div key={entry.name} className="rounded-2xl border border-slate-100 bg-slate-50 px-4 py-4 dark:border-slate-800 dark:bg-slate-800/60">
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex items-center gap-3">
                      <div className={`flex h-10 w-10 items-center justify-center rounded-2xl text-sm font-black text-white ${index === 0 ? 'bg-emerald-500' : 'bg-sky-500'}`}>
                        {index + 1}
                      </div>
                      <div>
                        <div className="text-sm font-black text-slate-900 dark:text-white">{entry.name}</div>
                        <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">State performance</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm font-black text-slate-900 dark:text-white">{formatInrCompact(entry.value)}</div>
                      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-emerald-500">
                        {selectedMetricTotal > 0 ? `${((entry.value / selectedMetricTotal) * 100).toFixed(1)}% of ${selectedYAxisLabel.toLowerCase()}` : 'No share yet'}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </AnalyticsPanel>
        </div>

        <div>
          <AnalyticsPanel title="Sales By Category" subtitle={`${selectedYAxisLabel} share by Category`} className="min-h-[28rem]" downloadRef={pieChartRef1} downloadFilename="category-pie-chart">
            <div className="relative h-60">
              <div className="w-full h-full">
                <PieAnalytics data={pieData} innerRadius={54} outerRadius={90} />
              </div>
            </div>
            <div className="mt-4 space-y-2">
              {(categoryOverviewRows.length > 0 ? categoryOverviewRows : pieData.map((entry) => ({ ...entry, share: selectedMetricTotal > 0 ? (entry.value / selectedMetricTotal) * 100 : 0 })))
                .slice(0, 4)
                .map((entry) => (
                  <div key={entry.name} className="flex items-center justify-between rounded-2xl border border-slate-100 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-600 dark:border-slate-800 dark:bg-slate-800/60 dark:text-slate-200">
                    <div className="flex items-center gap-3">
                      <span className="h-3 w-3 rounded-full" style={{ backgroundColor: entry.color }} />
                      <span className="font-bold text-slate-900 dark:text-white">{entry.name}</span>
                    </div>
                    <div className="text-right">
                      <div className="font-black text-slate-900 dark:text-white">{formatMetricValue(selectedYAxis, entry.value)}</div>
                      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{Number(entry.share || 0).toFixed(1)}% share</div>
                    </div>
                  </div>
                ))}
            </div>
          </AnalyticsPanel>
        </div>

        <div className="space-y-8">
          <AnalyticsPanel title="Histogram" subtitle={`${selectedYAxisLabel} distribution buckets`} className="min-h-[18rem]" downloadRef={histogramChartRef} downloadFilename="histogram-chart">
            <div className="relative h-44">
              <div ref={histogramChartRef} className="w-full h-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={histogramRanges}>
                    <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                    <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 10, fontWeight: 700 }} />
                    <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} allowDecimals={false} />
                    <Tooltip />
                    <Bar dataKey="count" fill="#f97316" radius={[10, 10, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </AnalyticsPanel>

          <AnalyticsPanel title="Overview" subtitle="Beneath summary strip" className="min-h-[34rem]" downloadRef={overviewPanelRef} downloadFilename="beneath-summary-strip" panelRef={overviewPanelRef}>
            <div className="overflow-hidden rounded-[1.75rem] border border-slate-200 dark:border-slate-800">
              <div className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr] gap-x-6 bg-slate-50 px-6 py-3 text-[10px] font-black uppercase tracking-[0.2em] text-slate-500 dark:bg-slate-800/70 dark:text-slate-300">
                <span className="block pl-2">Month</span>
                <span className="text-right">Sales</span>
                <span className="text-right">Profit</span>
                <span className="text-right">Margin</span>
              </div>
              <div className="bg-white dark:bg-slate-900">
                {overviewRows.map((entry) => (
                  <div key={entry.id || entry.month} className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr] gap-x-6 border-t border-slate-100 px-6 py-4 text-sm font-medium text-slate-600 dark:border-slate-800 dark:text-slate-200">
                    <span className="block whitespace-nowrap pl-2 font-bold text-slate-900 dark:text-white">{entry.month}</span>
                    <span className="whitespace-nowrap text-right">{entry.sales == null ? '-' : formatInrCompact(entry.sales)}</span>
                    <span className="whitespace-nowrap text-right">{entry.profit == null ? '-' : formatInrCompact(entry.profit)}</span>
                    <span className="whitespace-nowrap text-right font-black text-emerald-500">{entry.margin == null ? '-' : `${Number(entry.margin || 0).toFixed(1)}%`}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="mt-5 grid grid-cols-1 gap-4">
              <RealtimeMetric title="Total Rows" value={sortedEntries.length} detail="retrieved from backend" tone="rose" />
              <RealtimeMetric title="Calculated Total Sum" value={formatMetricValue(selectedYAxis, selectedMetricTotal)} detail={`${selectedYAxisLabel.toLowerCase()} across all rows`} tone="emerald" />
              <RealtimeMetric title="Autosync" value="Active" detail={projectId ? 'live backend project feed' : 'local analytics workspace'} tone="amber" />
            </div>
          </AnalyticsPanel>
        </div>
      </div>

      <AnalyticsPanel title="Real-Time Visualization" subtitle={`${projectionMetricLabels.join(' + ')} actual vs next ${projectionWindow}-month projection`} downloadRef={projectionChartRef} downloadFilename="projection-chart">
        <div className="relative h-56">
          <div ref={projectionChartRef} className="w-full h-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={projectionTrendRows}>
                <CartesianGrid vertical={false} strokeDasharray="3 3" opacity={0.15} />
                <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 11, fontWeight: 700 }} />
                <YAxis axisLine={false} tickLine={false} tick={{ fontSize: 11 }} />
                <Tooltip
                  formatter={(value, seriesName, item) => {
                    if (value == null) {
                      return ['-', seriesName];
                    }
                    const metricKey = String(item?.dataKey || '').split('_')[0];
                    return [formatMetricValue(metricKey, value), seriesName];
                  }}
                />
                <Legend />
                {projectionMetricKeys.map((metricKey) => {
                  const metricLabel = ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === metricKey)?.label || metricKey;
                  const strokeColor = '#2563eb';
                  return (
                    <Line
                      key={`${metricKey}-actual`}
                      type="monotone"
                      dataKey={`${metricKey}_actual`}
                      name={`${metricLabel} (Actual)`}
                      stroke={strokeColor}
                      strokeWidth={3}
                      dot={{ r: 4 }}
                      activeDot={{ r: 6 }}
                      connectNulls
                    />
                  );
                })}
                {projectionMetricKeys.map((metricKey) => {
                  const metricLabel = ANALYTICS_Y_AXIS_OPTIONS.find((option) => option.value === metricKey)?.label || metricKey;
                  const strokeColor = '#9333ea';
                  return (
                    <Line
                      key={`${metricKey}-projected`}
                      type="monotone"
                      dataKey={`${metricKey}_projected`}
                      name={`${metricLabel} (Projected)`}
                      stroke={strokeColor}
                      strokeWidth={3}
                      strokeDasharray="6 4"
                      dot={(props) => (props?.value == null ? null : <circle cx={props.cx} cy={props.cy} r={4} fill={strokeColor} />)}
                      activeDot={{ r: 6 }}
                      connectNulls
                    />
                  );
                })}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-5">
          {realtimeFeed.map((entry, index) => (
            <div key={entry.id} className="flex items-center justify-between rounded-2xl border border-slate-100 bg-slate-50 px-4 py-3 dark:border-slate-800 dark:bg-slate-800/50">
              <div className="flex items-center gap-3">
                <div className={`flex h-10 w-10 items-center justify-center rounded-2xl text-white ${index === 0 ? 'bg-emerald-500' : 'bg-sky-500'}`}>
                  <span className="material-symbols-outlined text-base">{index === 0 ? 'bolt' : 'monitoring'}</span>
                </div>
                <div>
                  <div className="text-sm font-black text-slate-900 dark:text-white">{entry.label}</div>
                  <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{entry.category} | {entry.region}</div>
                </div>
              </div>
              <div className="text-right">
                <div className="text-sm font-black text-slate-900 dark:text-white">{formatInrCompact(entry.revenue)}</div>
                <div className="text-[10px] font-black uppercase tracking-[0.2em] text-emerald-500">{entry.quantity} units</div>
              </div>
            </div>
          ))}
        </div>
      </AnalyticsPanel>

      <AnalyticsPanel title="Data Set Editor" subtitle="Choose axes, add rows, and drive the analytics board live">
        {isProjectLoading && (
          <div className="mb-5 rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm font-bold text-sky-700 dark:border-sky-900/40 dark:bg-sky-950/30 dark:text-sky-300">
            Loading selected project analytics...
          </div>
        )}
        <div className="mb-4 flex justify-end">
          <button
            type="button"
            onClick={handleClearImportedData}
            className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-2 text-xs font-black uppercase tracking-[0.2em] text-rose-600 transition-all hover:border-rose-300 hover:bg-rose-100 dark:border-rose-900/40 dark:bg-rose-950/30 dark:text-rose-300"
          >
            Clear Imported Data
          </button>
        </div>
        <div className="mb-6 rounded-[1.75rem] border border-slate-200 bg-slate-50 p-4 dark:border-slate-800 dark:bg-slate-800/60">
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-6 xl:items-end">
            <EditorField label="X Axis">
              <select value={selectedXAxis} onChange={(event) => setSelectedXAxis(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
                {ANALYTICS_X_AXIS_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </EditorField>
            <EditorField label="Y Axis (Multi Select)">
              <MultiSelectDropdown
                options={ANALYTICS_Y_AXIS_OPTIONS.map((option) => ({ value: option.value, label: option.label }))}
                selectedValues={selectedYAxes}
                onChange={(values) => setSelectedYAxes(values.length > 0 ? values : ['revenue'])}
                placeholder="Select one or more metrics"
                compactLabel
              />
            </EditorField>
            <EditorField label="Filter Category">
              <select value={selectedCategoryFilter} onChange={(event) => setSelectedCategoryFilter(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
                <option value="all">All Categories</option>
                {availableCategoryOptions.map((category) => <option key={category} value={category}>{category}</option>)}
              </select>
            </EditorField>
            <EditorField label="Filter Year">
              <select value={selectedYearFilter} onChange={(event) => setSelectedYearFilter(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
                <option value="all">All Years</option>
                {availableYearOptions.map((year) => <option key={year} value={year}>{year}</option>)}
              </select>
            </EditorField>
            <EditorField label="Month Range">
              <select value={selectedTimeWindow} onChange={(event) => setSelectedTimeWindow(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
                {ANALYTICS_TIME_WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </EditorField>
            <EditorField label="Projection Horizon">
              <select value={projectionWindow} onChange={(event) => setProjectionWindow(event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 dark:border-slate-700 dark:bg-slate-900 dark:text-white">
                {ANALYTICS_PROJECTION_WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </EditorField>
          </div>
          <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[1fr_1fr_1fr_auto] xl:items-end">
            <MultiSelectDropdown
              label="Products (Multi Select)"
              options={availableProductOptions.map((product) => ({ value: product, label: product }))}
              selectedValues={compareProducts}
              onChange={setCompareProducts}
              placeholder="All products"
            />
            <MultiSelectDropdown
              label="Filter Months (Multi Select)"
              options={availableMonthOptions.map((month) => ({ value: month, label: month }))}
              selectedValues={compareMonths}
              onChange={setCompareMonths}
              placeholder="All months"
            />
            <MultiSelectDropdown
              label="Regions (Multi Select)"
              options={availableRegionOptions.map((region) => ({ value: region, label: region }))}
              selectedValues={compareRegions}
              onChange={setCompareRegions}
              placeholder="All regions"
            />
            <div className="space-y-2">
              <div className="h-4 xl:h-5" />
              <button
                type="button"
                onClick={() => {
                  setSelectedCategoryFilter('all');
                  setSelectedYearFilter('all');
                  setSelectedTimeWindow('all');
                  setCompareProducts([]);
                  setCompareMonths([]);
                  setCompareRegions([]);
                }}
                className="h-12 rounded-2xl border border-slate-200 bg-white px-6 text-xs font-black uppercase tracking-[0.2em] text-slate-500 transition-all hover:border-sky-300 hover:text-sky-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300"
              >
                Clear
              </button>
            </div>
          </div>
        </div>
        <form onSubmit={handleAddEntry} className="grid grid-cols-1 gap-4 xl:grid-cols-8">
          <EditorField label="Product">
            <input value={formState.label} onChange={(event) => handleFormChange('label', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Product name" />
          </EditorField>
          <EditorField label="Category">
            <input value={formState.category} onChange={(event) => handleFormChange('category', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Category (e.g., Electronics)" />
          </EditorField>
          <EditorField label="Region">
            <input value={formState.region} onChange={(event) => handleFormChange('region', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Region (e.g., Tamil Nadu)" />
          </EditorField>
          <EditorField label="Month">
            <select value={formState.month} onChange={(event) => handleFormChange('month', event.target.value)} className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white">
              <option value="">Select month</option>
              {MONTH_ORDER.map((month) => <option key={month} value={month}>{month}</option>)}
            </select>
          </EditorField>
          <EditorField label="Year">
            <input value={formState.year} onChange={(event) => handleFormChange('year', event.target.value)} type="number" min="2000" max="2100" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Year (e.g., 2026)" />
          </EditorField>
          <EditorField label="Revenue">
            <input value={formState.revenue} onChange={(event) => handleFormChange('revenue', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Revenue value" />
          </EditorField>
          <EditorField label="Cost">
            <input value={formState.cost} onChange={(event) => handleFormChange('cost', event.target.value)} type="number" min="0" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Cost value" />
          </EditorField>
          <EditorField label="Quantity">
            <div className="flex gap-3">
              <input value={formState.quantity} onChange={(event) => handleFormChange('quantity', event.target.value)} type="number" min="1" className="h-12 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 text-sm font-medium outline-none transition-all focus:border-sky-400 focus:bg-white dark:border-slate-700 dark:bg-slate-800 dark:text-white" placeholder="Quantity" />
              <button type="submit" className="rounded-2xl bg-sky-600 px-5 text-sm font-black text-white shadow-lg shadow-sky-600/20 transition-all hover:-translate-y-0.5 hover:bg-sky-500">Add</button>
            </div>
          </EditorField>
        </form>

        <div className="mt-6 overflow-hidden rounded-[1.75rem] border border-slate-200 dark:border-slate-800">
          <div className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr_0.8fr_0.8fr] bg-slate-50 px-5 py-3 text-[10px] font-black uppercase tracking-[0.2em] text-slate-500 dark:bg-slate-800/70 dark:text-slate-300">
            <span>Product</span>
            <span>Category</span>
            <span>Region</span>
            <span>Revenue</span>
            <span>Cost</span>
            <span>Units</span>
          </div>
          <div className="max-h-72 overflow-y-auto bg-white dark:bg-slate-900">
            {sortedEntries.slice().reverse().map((entry) => (
              <div key={entry.id} className="grid grid-cols-[1.4fr_1fr_1fr_0.8fr_0.8fr_0.8fr] border-t border-slate-100 px-5 py-4 text-sm font-medium text-slate-600 dark:border-slate-800 dark:text-slate-200">
                <span className="font-bold text-slate-900 dark:text-white">{entry.label}</span>
                <span>{entry.category}</span>
                <span>{entry.region}</span>
                <span>{formatInrCompact(entry.revenue)}</span>
                <span>{formatInrCompact(entry.cost)}</span>
                <span>{entry.quantity}</span>
              </div>
            ))}
          </div>
        </div>
      </AnalyticsPanel>
    </div>
  );
}

// Layout components locally defined for Analytics
function AnalyticsStat({ title, value, sub }) {
  return (
    <div className="rounded-[1.75rem] border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/60">
      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{title}</div>
      <div className="mt-2 text-2xl font-black tracking-tighter text-slate-900 dark:text-white">{value}</div>
      <div className="mt-1 text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{sub}</div>
    </div>
  );
}

function AnalyticsPanel({ title, subtitle, children, className = '', downloadRef, downloadFilename, panelRef }) {
  const containerRef = useRef(null);

  return (
    <div
      ref={(node) => {
        containerRef.current = node;
        if (panelRef) {
          panelRef.current = node;
        }
      }}
      className={`relative rounded-[2.5rem] border border-slate-100 bg-white p-8 shadow-xl dark:border-slate-800 dark:bg-slate-900 ${className}`}
    >
      {downloadRef && <DownloadButton chartRef={containerRef} filename={downloadFilename || title.toLowerCase().replace(/\s+/g, '-')} />}
      <div className="mb-6 pr-16">
        <div className="text-[10px] font-black uppercase tracking-[0.3em] text-slate-400">{title}</div>
        <h3 className="mt-2 text-2xl font-black tracking-tighter text-slate-900 dark:text-white">{subtitle}</h3>
      </div>
      {children}
    </div>
  );
}

function EditorField({ label, children }) {
  return (
    <label className="block">
      <div className="mb-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{label}</div>
      {children}
    </label>
  );
}

function RealtimeMetric({ title, value, detail, tone }) {
  const toneClasses = {
    sky: 'bg-sky-50 text-sky-600 dark:bg-sky-950/30 dark:text-sky-300',
    rose: 'bg-rose-50 text-rose-600 dark:bg-rose-950/30 dark:text-rose-300',
    emerald: 'bg-emerald-50 text-emerald-600 dark:bg-emerald-950/30 dark:text-emerald-300',
    amber: 'bg-amber-50 text-amber-600 dark:bg-amber-950/30 dark:text-amber-300',
  };

  return (
    <div className="flex items-center justify-between rounded-[1.75rem] border border-slate-100 bg-slate-50 px-5 py-4 dark:border-slate-800 dark:bg-slate-800/50">
      <div>
        <div className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">{title}</div>
        <div className="mt-1 text-xs font-black uppercase tracking-[0.2em] text-slate-400">{detail}</div>
      </div>
      <div className={`rounded-2xl px-4 py-2 text-sm font-black ${toneClasses[tone]}`}>{value}</div>
    </div>
  );
}

export default AdvancedAnalyticsBoard;
