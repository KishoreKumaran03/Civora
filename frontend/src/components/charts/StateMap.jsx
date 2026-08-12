import { useState, useEffect } from 'react';
import { scaleLinear } from 'd3-scale';
import { INDIA_GEO_URL } from '../../constants/analyticsConstants';
import { formatInrCompact } from '../../utils/formatters';

export function StateMap({ stateData, hasData, showHeatScale = true, mapScale = 1 }) {
  const [hoveredState, setHoveredState] = useState(null);
  const [hoverPosition, setHoverPosition] = useState({ x: 0, y: 0 });
  const [mapGeometry, setMapGeometry] = useState(null);
  const [mapGeometryError, setMapGeometryError] = useState('');
  const stateEntries = Object.entries(stateData || {});
  const values = stateEntries.map(([, value]) => Number(value || 0)).filter(value => value > 0);
  const minValue = values.length ? Math.min(...values) : 0;
  const maxValue = values.length ? Math.max(...values) : 0;
  const colorScale = scaleLinear()
    .domain([minValue || 0, maxValue || 1])
    .range(['#dbeafe', '#1d4ed8']);

  useEffect(() => {
    let isMounted = true;

    const loadGeometry = async () => {
      try {
        setMapGeometryError('');
        const primary = await fetch(INDIA_GEO_URL);
        if (!primary.ok) {
          throw new Error(`Primary map fetch failed: ${primary.status}`);
        }
        const primaryJson = await primary.json();
        if (isMounted) {
          setMapGeometry(primaryJson);
        }
      } catch (primaryError) {
        try {
          const fallback = await fetch('/india-states.geojson');
          if (!fallback.ok) {
            throw new Error(`Fallback map fetch failed: ${fallback.status}`);
          }
          const fallbackJson = await fallback.json();
          if (isMounted) {
            setMapGeometry(fallbackJson);
          }
        } catch (fallbackError) {
          if (isMounted) {
            setMapGeometry(null);
            setMapGeometryError(String(fallbackError?.message || primaryError?.message || 'Unable to load map geometry'));
          }
        }
      }
    };

    loadGeometry();
    return () => {
      isMounted = false;
    };
  }, [mapScale]);

  return (
    <div className="relative h-full w-full rounded-[2rem] bg-[radial-gradient(circle_at_top,rgba(59,130,246,0.12),transparent_45%),linear-gradient(180deg,rgba(248,250,252,0.96),rgba(241,245,249,0.78))] p-4 dark:bg-[radial-gradient(circle_at_top,rgba(59,130,246,0.16),transparent_45%),linear-gradient(180deg,rgba(15,23,42,0.96),rgba(15,23,42,0.82))]">
      {(() => {
        const features = Array.isArray(mapGeometry?.features) ? mapGeometry.features : [];
        const viewBoxSize = 800;
        const padding = 42;

        if (features.length === 0) {
          return null;
        }

        let minLon = Number.POSITIVE_INFINITY;
        let maxLon = Number.NEGATIVE_INFINITY;
        let minLat = Number.POSITIVE_INFINITY;
        let maxLat = Number.NEGATIVE_INFINITY;

        const walk = (coords) => {
          if (!Array.isArray(coords)) return;
          if (typeof coords[0] === 'number' && typeof coords[1] === 'number') {
            const lon = Number(coords[0]);
            const lat = Number(coords[1]);
            if (Number.isFinite(lon) && Number.isFinite(lat)) {
              minLon = Math.min(minLon, lon);
              maxLon = Math.max(maxLon, lon);
              minLat = Math.min(minLat, lat);
              maxLat = Math.max(maxLat, lat);
            }
            return;
          }
          coords.forEach(walk);
        };

        features.forEach((feature) => walk(feature?.geometry?.coordinates));

        const lonRange = Math.max(0.0001, maxLon - minLon);
        const latRange = Math.max(0.0001, maxLat - minLat);
        const drawableWidth = viewBoxSize - padding * 2;
        const drawableHeight = viewBoxSize - padding * 2;
        const scale = Math.min(drawableWidth / lonRange, drawableHeight / latRange) * mapScale;
        const offsetX = (viewBoxSize - lonRange * scale) / 2;
        const offsetY = (viewBoxSize - latRange * scale) / 2;

        const projectPoint = (lon, lat) => {
          const x = offsetX + (lon - minLon) * scale;
          const y = offsetY + (maxLat - lat) * scale;
          return [x, y];
        };

        const ringToPath = (ring) => {
          if (!Array.isArray(ring) || ring.length === 0) return '';
          return ring.map((point, index) => {
            const [x, y] = projectPoint(Number(point[0]), Number(point[1]));
            return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
          }).join(' ') + ' Z';
        };

        const geometryToPath = (geometry) => {
          if (!geometry || !geometry.type || !geometry.coordinates) return '';
          if (geometry.type === 'Polygon') {
            return geometry.coordinates.map(ringToPath).join(' ');
          }
          if (geometry.type === 'MultiPolygon') {
            return geometry.coordinates.flatMap((polygon) => polygon.map(ringToPath)).join(' ');
          }
          return '';
        };

        return (
          <svg viewBox={`0 0 ${viewBoxSize} ${viewBoxSize}`} className="h-full w-full">
            {features.map((feature, index) => {
              const stateName = feature?.properties?.NAME_1 || feature?.properties?.name || `State ${index + 1}`;
              const value = Number(stateData?.[stateName] || 0);
              const fill = value > 0 ? colorScale(value) : '#bfdbfe';
              const d = geometryToPath(feature.geometry);
              if (!d) return null;

              return (
                <path
                  key={`${stateName}-${index}`}
                  d={d}
                  fill={fill}
                  stroke="#1e293b"
                  strokeWidth="0.9"
                  fillRule="evenodd"
                  onMouseEnter={() => setHoveredState({ name: stateName, value })}
                  onMouseMove={(event) => {
                    const nativeEvent = event.nativeEvent;
                    setHoverPosition({ x: nativeEvent.offsetX, y: nativeEvent.offsetY });
                  }}
                  onMouseLeave={() => setHoveredState(null)}
                />
              );
            })}
          </svg>
        );
      })()}

      {mapGeometryError && (
        <div className="pointer-events-none absolute left-1/2 top-1/2 w-[70%] -translate-x-1/2 -translate-y-1/2 rounded-2xl border border-rose-200 bg-white/90 px-4 py-3 text-center text-xs font-bold text-rose-600 shadow-sm">
          Map geometry is unavailable: {mapGeometryError}
        </div>
      )}

      {hoveredState && (
        <div
          className="pointer-events-none absolute rounded-xl border border-slate-200 bg-white/95 px-3 py-2 text-xs shadow-lg"
          style={{
            left: `${Math.min(Math.max(hoverPosition.x + 12, 12), 420)}px`,
            top: `${Math.min(Math.max(hoverPosition.y - 10, 12), 360)}px`,
          }}
        >
          <div className="font-black text-slate-900">{hoveredState.name}</div>
          <div className="mt-0.5 font-bold text-slate-600">
            {hoveredState.value > 0 ? formatInrCompact(hoveredState.value) : 'No uploaded data'}
          </div>
        </div>
      )}
    </div>
  );
}
export default StateMap;
