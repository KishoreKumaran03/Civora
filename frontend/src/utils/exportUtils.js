import { jsPDF } from 'jspdf';
import html2canvas from 'html2canvas';
import { formatInrCompact } from './formatters';
import { MONTH_ORDER } from '../constants/months';

export async function downloadChart(chartRef, filename = 'chart.png') {
  if (!chartRef?.current) return;

  const exportHiddenElements = Array.from(chartRef.current.querySelectorAll('[data-export-hidden="true"]'));
  const previousDisplayValues = exportHiddenElements.map((element) => element.style.display);

  exportHiddenElements.forEach((element) => {
    element.style.display = 'none';
  });

  try {
    const canvas = await html2canvas(chartRef.current, {
      backgroundColor: '#ffffff',
      scale: 2,
    });

    const link = document.createElement('a');
    link.href = canvas.toDataURL('image/png');
    link.download = `${filename}-${new Date().getTime()}.png`;
    link.click();
  } finally {
    exportHiddenElements.forEach((element, index) => {
      element.style.display = previousDisplayValues[index];
    });
  }
}

export function downloadProjectReportPdf(report) {
  const doc = new jsPDF();
  const pageWidth = doc.internal.pageSize.getWidth();
  const pageHeight = doc.internal.pageSize.getHeight();
  const marginX = 16;
  const contentWidth = pageWidth - (marginX * 2);
  let cursorY = 16;

  const ensureSpace = (requiredHeight) => {
    if (cursorY + requiredHeight > pageHeight - 14) {
      doc.addPage();
      cursorY = 16;
    }
  };

  const writeLine = (text, size = 11, gap = 6) => {
    doc.setFontSize(size);
    doc.setTextColor(15, 23, 42);
    const lines = doc.splitTextToSize(String(text || ''), contentWidth);
    ensureSpace((lines.length * gap) + 2);
    doc.text(lines, marginX, cursorY);
    cursorY += lines.length * gap;
  };

  const writeSectionTitle = (title) => {
    ensureSpace(10);
    doc.setFontSize(13);
    doc.setTextColor(30, 41, 59);
    doc.text(title, marginX, cursorY);
    cursorY += 7;
  };

  const drawTrendChart = (series) => {
    const chartHeight = 72;
    ensureSpace(chartHeight + 8);

    const x = marginX;
    const y = cursorY;
    const w = contentWidth;
    const h = chartHeight;

    doc.setDrawColor(226, 232, 240);
    doc.setFillColor(248, 250, 252);
    doc.roundedRect(x, y, w, h, 4, 4, 'FD');

    doc.setFontSize(11);
    doc.setTextColor(30, 41, 59);
    doc.text('Revenue & Profit Trend', x + 4, y + 7);

    const points = Array.isArray(series) ? series.slice(-8) : [];
    if (points.length === 0) {
      doc.setFontSize(10);
      doc.setTextColor(100, 116, 139);
      doc.text('No trend data available.', x + 4, y + 18);
      cursorY += chartHeight + 6;
      return;
    }

    const plotX = x + 10;
    const plotY = y + 12;
    const plotW = w - 18;
    const plotH = h - 24;
    const maxRevenue = Math.max(...points.map((point) => Number(point.revenue || 0)), 1);
    const maxProfit = Math.max(...points.map((point) => Number(point.profit || 0)), 1);
    const slotW = plotW / points.length;
    const barW = Math.max(4, Math.min(12, slotW * 0.5));

    doc.setDrawColor(203, 213, 225);
    doc.line(plotX, plotY + plotH, plotX + plotW, plotY + plotH);

    const linePoints = [];
    points.forEach((point, index) => {
      const revenue = Math.max(0, Number(point.revenue || 0));
      const profit = Math.max(0, Number(point.profit || 0));
      const centerX = plotX + (index * slotW) + (slotW / 2);
      const barHeight = (revenue / maxRevenue) * (plotH - 6);
      const barX = centerX - (barW / 2);
      const barY = plotY + plotH - barHeight;

      doc.setFillColor(56, 189, 248);
      doc.roundedRect(barX, barY, barW, barHeight, 1.5, 1.5, 'F');

      const lineY = plotY + plotH - ((profit / maxProfit) * (plotH - 6));
      linePoints.push({ x: centerX, y: lineY });

      doc.setFontSize(8);
      doc.setTextColor(71, 85, 105);
      doc.text(String(point.name || ''), centerX, plotY + plotH + 5, { align: 'center' });
    });

    doc.setDrawColor(245, 158, 11);
    doc.setLineWidth(0.8);
    linePoints.forEach((point, index) => {
      if (index > 0) {
        const previous = linePoints[index - 1];
        doc.line(previous.x, previous.y, point.x, point.y);
      }
      doc.setFillColor(245, 158, 11);
      doc.circle(point.x, point.y, 0.9, 'F');
    });

    doc.setFontSize(8);
    doc.setTextColor(56, 189, 248);
    doc.text('Revenue', x + w - 36, y + 7);
    doc.setTextColor(245, 158, 11);
    doc.text('Profit', x + w - 16, y + 7);

    cursorY += chartHeight + 6;
  };

  const drawCategoryChart = (series) => {
    const topCategories = Array.isArray(series) ? series.slice(0, 5) : [];
    const chartHeight = 62;
    ensureSpace(chartHeight + 8);

    const x = marginX;
    const y = cursorY;
    const w = contentWidth;
    const h = chartHeight;

    doc.setDrawColor(226, 232, 240);
    doc.setFillColor(248, 250, 252);
    doc.roundedRect(x, y, w, h, 4, 4, 'FD');

    doc.setFontSize(11);
    doc.setTextColor(30, 41, 59);
    doc.text('Top Category Share', x + 4, y + 7);

    if (topCategories.length === 0) {
      doc.setFontSize(10);
      doc.setTextColor(100, 116, 139);
      doc.text('No category data available.', x + 4, y + 18);
      cursorY += chartHeight + 6;
      return;
    }

    const maxValue = Math.max(...topCategories.map((row) => Number(row.value || 0)), 1);
    topCategories.forEach((row, index) => {
      const lineY = y + 14 + (index * 9);
      const value = Number(row.value || 0);
      const barMaxW = 76;
      const barW = (value / maxValue) * barMaxW;
      const barX = x + 74;

      doc.setFontSize(9);
      doc.setTextColor(51, 65, 85);
      doc.text(String(row.name || '-'), x + 4, lineY);

      doc.setFillColor(226, 232, 240);
      doc.roundedRect(barX, lineY - 3, barMaxW, 4.5, 1, 1, 'F');
      doc.setFillColor(56, 189, 248);
      doc.roundedRect(barX, lineY - 3, Math.max(1, barW), 4.5, 1, 1, 'F');

      doc.setTextColor(30, 41, 59);
      doc.text(formatInrCompact(value), x + w - 4, lineY, { align: 'right' });
    });

    cursorY += chartHeight + 6;
  };

  doc.setFontSize(18);
  doc.setTextColor(15, 23, 42);
  doc.text(`${report.project.name} Analytics Report`, marginX, cursorY);
  cursorY += 8;

  doc.setFontSize(10);
  doc.setTextColor(100, 116, 139);
  doc.text(`Generated from Advanced Analytics for ${report.project.name}`, marginX, cursorY);
  cursorY += 8;

  writeSectionTitle('Past / Present / Future');
  report.highlights.slice(0, 1).forEach((line) => writeLine(line, 11, 6));
  writeLine(report.highlights[1], 11, 6);
  writeLine(report.highlights[2], 11, 6);

  writeSectionTitle('Core Metrics');
  writeLine(`Revenue: ${formatInrCompact(report.totals.revenue)} | Profit: ${formatInrCompact(report.totals.profit)} | Margin: ${report.margin.toFixed(1)}% | Units: ${report.totals.quantity}`, 10, 6);
  writeLine(`Top Category: ${report.topCategory ? `${report.topCategory[0]} (${formatInrCompact(report.topCategory[1])})` : 'N/A'}`, 10, 6);
  writeLine(`Top Region: ${report.topRegion ? `${report.topRegion[0]} (${formatInrCompact(report.topRegion[1])})` : 'N/A'}`, 10, 6);

  writeSectionTitle('Graph Snapshot');
  drawTrendChart(report.trendSeries);
  drawCategoryChart(report.categorySeries);

  writeSectionTitle('SWOT Analysis');
  writeLine(`Strengths: ${report.strengths.join(' ')}`, 10, 6);
  writeLine(`Weaknesses: ${report.weaknesses.join(' ')}`, 10, 6);
  writeLine(`Opportunities: ${report.opportunities.join(' ')}`, 10, 6);
  writeLine(`Threats: ${report.threats.join(' ')}`, 10, 6);

  doc.save(`${report.project.name.replace(/[^a-z0-9]+/gi, '_').toLowerCase()}_report.pdf`);
}
