import React from 'react';
import { Line } from 'react-chartjs-2';
import { chartColors, timeSeriesOptions } from './ChartUtils';

interface TimeSeriesChartProps {
  data: {
    timestamps: string[];
    datasets: {
      label: string;
      data: number[];
      color?: string;
    }[];
  };
  title?: string;
  height?: number;
}

export const TimeSeriesChart: React.FC<TimeSeriesChartProps> = ({
  data,
  title,
  height = 300,
}) => {
  // Add safety checks for data
  if (!data || !data.datasets || !Array.isArray(data.datasets) || !data.timestamps) {
    return (
      <div className="w-full flex items-center justify-center" style={{ height }}>
        <p className="text-gray-500">No data available</p>
      </div>
    );
  }

  const chartData = {
    labels: data.timestamps.map(ts => new Date(ts)),
    datasets: data.datasets.map((dataset, index) => ({
      label: dataset.label,
      data: dataset.data,
      borderColor: dataset.color || Object.values(chartColors)[index % Object.values(chartColors).length],
      backgroundColor: dataset.color || Object.values(chartColors)[index % Object.values(chartColors).length] + '20',
      tension: 0.4,
      fill: false,
      pointRadius: 3,
      pointHoverRadius: 6,
    })),
  };

  return (
    <div className="w-full" style={{ height }}>
      {title && (
        <h3 className="text-lg font-semibold mb-4 text-gray-800">{title}</h3>
      )}
      <Line data={chartData} options={timeSeriesOptions} />
    </div>
  );
};