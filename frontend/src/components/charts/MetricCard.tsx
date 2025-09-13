import React from 'react';
import { Doughnut } from 'react-chartjs-2';
import { chartColors } from './ChartUtils';

interface MetricCardProps {
  title: string;
  value: number;
  total: number;
  color?: string;
  format?: 'number' | 'percentage';
  icon?: React.ReactNode;
}

export const MetricCard: React.FC<MetricCardProps> = ({
  title,
  value,
  total,
  color = chartColors.primary,
  format = 'number',
  icon,
}) => {
  // Cap percentage at 100% and handle already-percentage values
  const isPercentageFormat = format === 'percentage';
  const displayPercentage = isPercentageFormat 
    ? Math.min(Math.max(value, 0), 100)  // Cap between 0-100%
    : total > 0 ? Math.min((value / total) * 100, 100) : 0;
  
  const chartData = {
    datasets: [
      {
        data: isPercentageFormat 
          ? [displayPercentage, 100 - displayPercentage]
          : [value, Math.max(total - value, 0)],
        backgroundColor: [color, '#e5e7eb'],
        borderWidth: 0,
        cutout: '70%',
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        enabled: false,
      },
    },
  };

  const displayValue = isPercentageFormat 
    ? `${displayPercentage.toFixed(1)}%`
    : value.toLocaleString();

  return (
    <div className="bg-white rounded-lg shadow-sm border p-6">
      <div className="flex items-center justify-between">
        <div className="flex-1">
          <p className="text-sm font-medium text-gray-600 mb-1">{title}</p>
          <div className="flex items-center gap-2">
            {icon && <div className="text-gray-400">{icon}</div>}
            <p className="text-2xl font-bold text-gray-900">{displayValue}</p>
          </div>
          {format === 'number' && total > 0 && (
            <p className="text-xs text-gray-500 mt-1">
              {displayPercentage.toFixed(1)}% of {total.toLocaleString()}
            </p>
          )}
        </div>
        <div className="w-16 h-16 ml-4">
          <Doughnut data={chartData} options={options} />
        </div>
      </div>
    </div>
  );
};