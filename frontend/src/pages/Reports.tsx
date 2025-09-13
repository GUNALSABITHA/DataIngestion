import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { 
  BarChart3, 
  TrendingUp, 
  Activity, 
  Database, 
  CheckCircle, 
  XCircle, 
  AlertTriangle,
  RefreshCw
} from "lucide-react";
import { TimeSeriesChart, BarChart, PieChart, MetricCard } from "@/components/charts";
import { toast } from "@/hooks/use-toast";

interface DashboardStats {
  job_statistics: Array<{
    status: string;
    count: number;
    avg_duration: number;
  }>;
  quality_trends: Array<{
    date: string;
    completeness: number;
    validity: number;
    consistency: number;
    records_processed: number;
  }>;
  error_distribution: Array<{
    error_type: string;
    count: number;
  }>;
  summary: {
    total_jobs: number;
    avg_quality_score: number;
    total_errors: number;
  };
}

interface ProcessingTimeline {
  timeline: Array<{
    timestamp: string;
    status: string;
    count: number;
    avg_records: number;
    total_records: number;
  }>;
  period_days: number;
}

interface DataSourcesStats {
  data_sources: Array<{
    source_name: string;
    source_type: string;
    total_jobs: number;
    successful_jobs: number;
    avg_records: number;
    last_processed: string;
  }>;
}

interface QualityMetrics {
  quality_metrics: Array<{
    date: string;
    metric_name: string;
    avg_value: number;
    min_value: number;
    max_value: number;
    sample_count: number;
  }>;
  validation_results: Array<{
    validation_rule: string;
    passed: number;
    failed: number;
  }>;
  period_days: number;
}

const Reports = () => {
  const [dashboardStats, setDashboardStats] = useState<DashboardStats | null>(null);
  const [timeline, setTimeline] = useState<ProcessingTimeline | null>(null);
  const [dataSources, setDataSources] = useState<DataSourcesStats | null>(null);
  const [qualityMetrics, setQualityMetrics] = useState<QualityMetrics | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchData = async () => {
    try {
      setLoading(true);
      
      // Fetch real data from API endpoints
      const [statsRes, timelineRes, sourcesRes, qualityRes] = await Promise.all([
        fetch('http://localhost:8000/api/reports/dashboard-stats'),
        fetch('http://localhost:8000/api/reports/processing-timeline?days=7'),
        fetch('http://localhost:8000/api/reports/data-sources'),
        fetch('http://localhost:8000/api/reports/quality-metrics?days=30')
      ]);

      if (statsRes.ok) {
        const statsData = await statsRes.json();
        setDashboardStats(statsData);
      } else {
        console.error('Failed to fetch dashboard stats:', statsRes.status, statsRes.statusText);
        setDashboardStats(null);
      }
      
      if (timelineRes.ok) {
        const timelineData = await timelineRes.json();
        setTimeline(timelineData);
      } else {
        console.error('Failed to fetch timeline:', timelineRes.status, timelineRes.statusText);
        setTimeline(null);
      }
      
      if (sourcesRes.ok) {
        const sourcesData = await sourcesRes.json();
        setDataSources(sourcesData);
      } else {
        console.error('Failed to fetch data sources:', sourcesRes.status, sourcesRes.statusText);
        setDataSources(null);
      }
      
      if (qualityRes.ok) {
        const qualityData = await qualityRes.json();
        setQualityMetrics(qualityData);
      } else {
        console.error('Failed to fetch quality metrics:', qualityRes.status, qualityRes.statusText);
        setQualityMetrics(null);
      }

    } catch (error) {
      console.error('Failed to fetch reports:', error);
      setDashboardStats(null);
      setTimeline(null);
      setDataSources(null);
      setQualityMetrics(null);
      
      toast({
        title: "Failed to load reports",
        description: error instanceof Error ? error.message : "Unknown error occurred",
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="flex items-center gap-2">
          <RefreshCw className="h-4 w-4 animate-spin" />
          <span>Loading reports...</span>
        </div>
      </div>
    );
  }

  // Check if we have any data
  const hasData = dashboardStats || timeline || dataSources || qualityMetrics;

  if (!hasData) {
    return (
      <div className="container mx-auto p-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Data Pipeline Reports</h1>
            <p className="text-gray-600 mt-1">Analytics and insights from your data ingestion pipeline</p>
          </div>
          <Button onClick={fetchData} variant="outline">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        </div>

        <Card className="p-12 text-center">
          <div className="flex flex-col items-center gap-4">
            <Database className="h-16 w-16 text-gray-400" />
            <div>
              <h2 className="text-xl font-semibold text-gray-900 mb-2">No Data Available</h2>
              <p className="text-gray-600 mb-4">
                No validation jobs have been processed yet. Upload and validate some files to see reports.
              </p>
              <Badge variant="outline" className="mb-4">
                API: {navigator.onLine ? "Connected" : "Offline"}
              </Badge>
              <div className="flex gap-2 justify-center">
                <Button onClick={() => window.location.href = '/upload'} variant="default">
                  Upload Files
                </Button>
                <Button onClick={fetchData} variant="outline">
                  <RefreshCw className="h-4 w-4 mr-2" />
                  Retry
                </Button>
              </div>
            </div>
          </div>
        </Card>
      </div>
    );
  }

  // Prepare chart data
  const jobStatusData = dashboardStats ? {
    labels: dashboardStats.job_statistics.map(stat => stat.status),
    values: dashboardStats.job_statistics.map(stat => stat.count),
    colors: ['#10b981', '#f59e0b', '#ef4444', '#6b7280']
  } : { labels: [], values: [], colors: [] };

  const qualityTrendData = dashboardStats ? {
    timestamps: dashboardStats.quality_trends.map(trend => trend.date),
    datasets: [
      {
        label: 'Completeness',
        data: dashboardStats.quality_trends.map(trend => trend.completeness),
        color: '#3b82f6'
      },
      {
        label: 'Validity', 
        data: dashboardStats.quality_trends.map(trend => trend.validity),
        color: '#10b981'
      },
      {
        label: 'Consistency',
        data: dashboardStats.quality_trends.map(trend => trend.consistency),
        color: '#8b5cf6'
      }
    ]
  } : { timestamps: [], datasets: [] };

  const errorDistributionData = dashboardStats ? {
    labels: dashboardStats.error_distribution.map(error => error.error_type),
    datasets: [{
      label: 'Error Count',
      data: dashboardStats.error_distribution.map(error => error.count),
      color: '#ef4444'
    }]
  } : { labels: [], datasets: [] };

  const processingVolumeData = timeline ? {
    timestamps: timeline.timeline.map(item => item.timestamp),
    datasets: [{
      label: 'Records Processed',
      data: timeline.timeline.map(item => item.total_records),
      color: '#06b6d4'
    }]
  } : { timestamps: [], datasets: [] };

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Data Pipeline Reports</h1>
          <p className="text-gray-600 mt-1">Analytics and insights from your data ingestion pipeline</p>
        </div>
        <Button onClick={fetchData} variant="outline">
          <RefreshCw className="h-4 w-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* Summary Cards */}
      {dashboardStats && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <MetricCard
            title="Total Jobs"
            value={dashboardStats.summary.total_jobs}
            total={dashboardStats.summary.total_jobs}
            color="#3b82f6"
            icon={<Activity />}
          />
          <MetricCard
            title="Average Quality Score"
            value={dashboardStats.summary.avg_quality_score}
            total={100}
            format="percentage"
            color="#10b981"
            icon={<CheckCircle />}
          />
          <MetricCard
            title="Total Errors"
            value={dashboardStats.summary.total_errors}
            total={dashboardStats.summary.total_jobs}
            color="#ef4444"
            icon={<XCircle />}
          />
        </div>
      )}

      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="quality">Quality</TabsTrigger>
          <TabsTrigger value="performance">Performance</TabsTrigger>
          <TabsTrigger value="sources">Data Sources</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <BarChart3 className="h-5 w-5" />
                  Job Status Distribution
                </CardTitle>
                <CardDescription>
                  Distribution of job statuses over the last 30 days
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-64">
                  {dashboardStats ? (
                    <PieChart data={jobStatusData} />
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-500">
                      <div className="text-center">
                        <BarChart3 className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p className="text-sm">No job data available</p>
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <TrendingUp className="h-5 w-5" />
                  Quality Trends
                </CardTitle>
                <CardDescription>
                  Data quality metrics over time
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-64">
                  {dashboardStats ? (
                    <TimeSeriesChart data={qualityTrendData} />
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-500">
                      <div className="text-center">
                        <TrendingUp className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p className="text-sm">No quality trend data available</p>
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

            <Card className="lg:col-span-2">
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <Activity className="h-5 w-5" />
                  Processing Volume
                </CardTitle>
                <CardDescription>
                  Records processed over the last 7 days
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-64">
                  {timeline ? (
                    <TimeSeriesChart data={processingVolumeData} />
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-500">
                      <div className="text-center">
                        <Activity className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p className="text-sm">No processing data available</p>
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="quality" className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2">
                  <AlertTriangle className="h-5 w-5" />
                  Error Distribution
                </CardTitle>
                <CardDescription>
                  Most common validation errors
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="h-64">
                  {dashboardStats ? (
                    <BarChart data={errorDistributionData} horizontal />
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-500">
                      <div className="text-center">
                        <AlertTriangle className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p className="text-sm">No error data available</p>
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Validation Rules Performance</CardTitle>
                <CardDescription>
                  Pass/fail rates for validation rules
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {qualityMetrics?.validation_results.slice(0, 5).map((rule) => {
                    const total = rule.passed + rule.failed;
                    const passRate = total > 0 ? (rule.passed / total) * 100 : 0;
                    
                    return (
                      <div key={rule.validation_rule} className="space-y-2">
                        <div className="flex justify-between items-center">
                          <span className="text-sm font-medium">{rule.validation_rule}</span>
                          <span className="text-sm text-gray-500">{passRate.toFixed(1)}%</span>
                        </div>
                        <div className="w-full bg-gray-200 rounded-full h-2">
                          <div
                            className="bg-green-600 h-2 rounded-full"
                            style={{ width: `${passRate}%` }}
                          />
                        </div>
                        <div className="flex justify-between text-xs text-gray-500">
                          <span>{rule.passed} passed</span>
                          <span>{rule.failed} failed</span>
                        </div>
                      </div>
                    );
                  }) || (
                    <div className="flex items-center justify-center py-8 text-gray-500">
                      <div className="text-center">
                        <CheckCircle className="h-8 w-8 mx-auto mb-2 opacity-50" />
                        <p className="text-sm">No validation rules data available</p>
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        <TabsContent value="performance" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Performance Metrics</CardTitle>
              <CardDescription>
                Job duration and throughput statistics
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                {dashboardStats?.job_statistics.map((stat) => (
                  <div key={stat.status} className="p-4 border rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      {stat.status === 'completed' && <CheckCircle className="h-4 w-4 text-green-500" />}
                      {stat.status === 'failed' && <XCircle className="h-4 w-4 text-red-500" />}
                      {stat.status === 'processing' && <Activity className="h-4 w-4 text-blue-500" />}
                      <span className="font-medium capitalize">{stat.status}</span>
                    </div>
                    <div className="text-2xl font-bold">{stat.count}</div>
                    <div className="text-sm text-gray-500">
                      Avg: {stat.avg_duration ? `${stat.avg_duration.toFixed(1)}s` : 'N/A'}
                    </div>
                  </div>
                )) || (
                  <div className="col-span-3 flex items-center justify-center py-12 text-gray-500">
                    <div className="text-center">
                      <Activity className="h-12 w-12 mx-auto mb-4 opacity-50" />
                      <h3 className="text-lg font-medium mb-2">No Performance Data</h3>
                      <p className="text-sm">No performance metrics available yet.</p>
                    </div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="sources" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Database className="h-5 w-5" />
                Data Sources Statistics
              </CardTitle>
              <CardDescription>
                Performance metrics by data source
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {dataSources?.data_sources.map((source) => {
                  const successRate = source.total_jobs > 0 
                    ? (source.successful_jobs / source.total_jobs) * 100 
                    : 0;

                  return (
                    <div key={source.source_name} className="border rounded-lg p-4">
                      <div className="flex justify-between items-start mb-2">
                        <div>
                          <h3 className="font-medium">{source.source_name}</h3>
                          <Badge variant="secondary" className="text-xs">
                            {source.source_type}
                          </Badge>
                        </div>
                        <div className="text-right">
                          <div className="text-sm text-gray-500">Success Rate</div>
                          <div className="text-lg font-bold text-green-600">
                            {successRate.toFixed(1)}%
                          </div>
                        </div>
                      </div>
                      
                      <div className="grid grid-cols-3 gap-4 text-sm">
                        <div>
                          <div className="text-gray-500">Total Jobs</div>
                          <div className="font-medium">{source.total_jobs}</div>
                        </div>
                        <div>
                          <div className="text-gray-500">Avg Records</div>
                          <div className="font-medium">
                            {source.avg_records ? Math.round(source.avg_records).toLocaleString() : 'N/A'}
                          </div>
                        </div>
                        <div>
                          <div className="text-gray-500">Last Processed</div>
                          <div className="font-medium">
                            {source.last_processed 
                              ? new Date(source.last_processed).toLocaleDateString()
                              : 'Never'
                            }
                          </div>
                        </div>
                      </div>
                    </div>
                  );
                }) || (
                  <div className="flex items-center justify-center py-12 text-gray-500">
                    <div className="text-center">
                      <Database className="h-12 w-12 mx-auto mb-4 opacity-50" />
                      <h3 className="text-lg font-medium mb-2">No Data Sources</h3>
                      <p className="text-sm">No data sources have been processed yet.</p>
                    </div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default Reports;