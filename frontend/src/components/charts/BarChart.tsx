import React from 'react';
import { Bar } from 'react-chartjs-2';
import { chartColors, defaultChartOptions } from './ChartUtils';

interface BarChartProps {
  data: {
    labels: string[];
    datasets: {
      label: string;
      data: number[];
      color?: string;
    }[];
  };
  title?: string;
  height?: number;
  horizontal?: boolean;
}

export const BarChart: React.FC<BarChartProps> = ({
  data,
  title,
  height = 300,
  horizontal = false,
}) => {
  const chartData = {
    labels: data.labels,
    datasets: data.datasets.map((dataset, index) => ({
      label: dataset.label,
      data: dataset.data,
      backgroundColor: dataset.color || Object.values(chartColors)[index % Object.values(chartColors).length],
      borderColor: dataset.color || Object.values(chartColors)[index % Object.values(chartColors).length],
      borderWidth: 1,
      borderRadius: 4,
    })),
  };

  const options = {
    ...defaultChartOptions,
    indexAxis: horizontal ? 'y' as const : 'x' as const,
    plugins: {
      ...defaultChartOptions.plugins,
      title: {
        display: !!title,
        text: title,
      },
    },
  };

  return (
    <div className="w-full" style={{ height }}>
      {title && !options.plugins?.title?.display && (
        <h3 className="text-lg font-semibold mb-4 text-gray-800">{title}</h3>
      )}
      <Bar data={chartData} options={options} />
    </div>
  );
};