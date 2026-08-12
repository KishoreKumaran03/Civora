import { MONTH_ORDER } from '../constants/months';

export function getSequentialBatchPeriod(startMonth, startYear, offset) {
  const initialMonthIndex = Math.max(0, MONTH_ORDER.indexOf(startMonth));
  const initialYear = Number.parseInt(startYear, 10) || new Date().getFullYear();
  const totalMonthIndex = initialMonthIndex + offset;
  const monthIndex = ((totalMonthIndex % 12) + 12) % 12;
  const yearOffset = Math.floor(totalMonthIndex / 12);

  return {
    month: MONTH_ORDER[monthIndex],
    year: String(initialYear + yearOffset),
  };
}

export function buildBatchItems(files, startMonth, startYear, startIndex = 0) {
  return files.map((file, index) => {
    const period = getSequentialBatchPeriod(startMonth, startYear, startIndex + index);
    return {
      id: `${Date.now()}-${startIndex + index}-${file.name}`,
      file,
      name: file.name,
      month: period.month,
      year: period.year,
    };
  });
}
