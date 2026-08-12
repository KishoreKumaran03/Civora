export const INDIA_GEO_URL = '/india-states-simplified.geojson';

export const YEAR_RANGE_START = 2020;
export const YEAR_RANGE_END = 2030;

export const ADVANCED_ANALYTICS_SEED = [
  { id: 1, label: 'North Star Phones', category: 'Electronics', region: 'Tamil Nadu', month: 'March', year: 2024, revenue: 125000, cost: 76000, quantity: 14, recordedAt: '2024-03-01T09:30:00' },
  { id: 2, label: 'Kerala Living Set', category: 'Furniture', region: 'Kerala', month: 'March', year: 2024, revenue: 84000, cost: 47000, quantity: 7, recordedAt: '2024-03-01T11:00:00' },
  { id: 3, label: 'Metro Smart Hub', category: 'Electronics', region: 'NCT of Delhi', month: 'March', year: 2024, revenue: 98000, cost: 59000, quantity: 10, recordedAt: '2024-03-01T13:15:00' },
  { id: 4, label: 'Odisha Decor Pack', category: 'Furniture', region: 'Odisha', month: 'March', year: 2024, revenue: 61000, cost: 33000, quantity: 9, recordedAt: '2024-03-01T15:10:00' },
  { id: 5, label: 'Bengaluru Sound Grid', category: 'Electronics', region: 'Karnataka', month: 'March', year: 2024, revenue: 142000, cost: 87000, quantity: 16, recordedAt: '2024-03-01T17:45:00' },
  { id: 6, label: 'South Essentials', category: 'Others', region: 'Tamil Nadu', month: 'March', year: 2024, revenue: 52000, cost: 26000, quantity: 11, recordedAt: '2024-03-02T09:05:00' }
];

export const years = Array.from(
  { length: YEAR_RANGE_END - YEAR_RANGE_START + 1 },
  (_, index) => String(YEAR_RANGE_START + index)
);

export const ANALYTICS_X_AXIS_OPTIONS = [
  { value: 'month', label: 'Month' },
  { value: 'category', label: 'Category' },
  { value: 'region', label: 'Region' },
  { value: 'year', label: 'Year' },
  { value: 'label', label: 'Product' }
];

export const ANALYTICS_Y_AXIS_OPTIONS = [
  { value: 'revenue', label: 'Revenue' },
  { value: 'cost', label: 'Cost' },
  { value: 'quantity', label: 'Quantity' },
  { value: 'profit', label: 'Profit' }
];

export const ANALYTICS_TIME_WINDOW_OPTIONS = [
  { value: 'all', label: 'All Data' },
  { value: '3', label: 'Last 3 Months' },
  { value: '6', label: 'Last 6 Months' },
  { value: '12', label: 'Last 12 Months' },
  { value: 'year', label: 'Latest Year' }
];

export const ANALYTICS_PROJECTION_WINDOW_OPTIONS = [
  { value: '3', label: '3 Months' },
  { value: '6', label: '6 Months' },
  { value: '12', label: '12 Months' }
];

export const IMPORT_MAPPING_FIELDS = [
  { key: 'Product', label: 'Product Column', helper: 'Name of the product or item', required: true },
  { key: 'Category', label: 'Category Column', helper: 'Group, type, or product family', required: false },
  { key: 'Region', label: 'Region Column', helper: 'State, city, region, or area', required: true },
  { key: 'Revenue', label: 'Revenue Column', helper: 'Sales, revenue, income, or amount', required: true },
  { key: 'Cost', label: 'Cost Column', helper: 'Cost, expense, or spend', required: true },
  { key: 'Quantity', label: 'Quantity Column', helper: 'Units, quantity, or item count', required: true }
];
