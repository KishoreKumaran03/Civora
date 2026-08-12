export function formatInrCompact(value) {
  const amount = Number(value || 0);
  if (amount >= 10000000) {
    return `Rs ${(amount / 10000000).toFixed(2)} Cr`;
  }
  if (amount >= 100000) {
    return `Rs ${(amount / 100000).toFixed(1)} L`;
  }

  return `Rs ${amount.toLocaleString('en-IN', { maximumFractionDigits: 0 })}`;
}

export function formatMetricValue(metric, value) {
  if (metric === 'quantity') {
    return Number(value || 0).toLocaleString('en-IN');
  }

  return formatInrCompact(value);
}
