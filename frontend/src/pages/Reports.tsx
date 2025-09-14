import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { 
  TrendingUp, 
  Activity, 
  Database, 
  RefreshCw,
  Eye,
  FileText,
  AlertTriangle,
  BarChart3,
  PieChart as PieChartIcon,
  X
} from "lucide-react";
import { TimeSeriesChart, BarChart, PieChart, MetricCard } from "@/components/charts/index";
import { toast } from "@/hooks/use-toast";

interface JobsOverview {
  jobs: Array<{
    job_id: string;
    job_name: string;
    status: string;
    total_records: number;
    valid_records: number;
    invalid_records: number;
    quarantined_records: number;
    success_rate_pct: number;
    invalid_pct: number;
    missing_pct: number;
    quarantined_pct: number;
    quality_score_pct: number;
    created_at?: string;
    started_at?: string;
    completed_at?: string;
  }>;
  period_days: number;
}

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

interface JobReport {
  job_info: {
    job_id: string;
    job_name: string;
    job_type: string;
    status: string;
    created_at: string;
    started_at: string;
    completed_at: string;
    source_name: string;
    source_type: string;
    source_format: string;
    error_message: string;
  };
  statistics: {
    total_records: number;
    valid_records: number;
    invalid_records: number;
    quarantined_records: number;
    success_rate: number;
    avg_quality_score: number;
  };
  validation_results: Array<{
    result_id: string;
    rule_name: string;
    rule_type: string;
    field_name: string;
    records_checked: number;
    records_passed: number;
    records_failed: number;
    success_rate: number;
    error_details: string;
    validated_at: string;
  }>;
  quality_metrics: Array<{
    metric_id: string;
    metric_name: string;
    field_name: string;
    metric_value: number;
    metric_type: string;
    calculated_at: string;
    details: string;
  }>;
}

const Reports = () => {
  const [jobsOverview, setJobsOverview] = useState<JobsOverview | null>(null);
  const [dashboardStats, setDashboardStats] = useState<DashboardStats | null>(null);
  const [selectedJobReport, setSelectedJobReport] = useState<JobReport | null>(null);
  const [loading, setLoading] = useState(true);
  const [isReportModalOpen, setIsReportModalOpen] = useState(false);

  // Mock data generators for offline/error states
  const getMockTimeSeriesData = () => ({
    timestamps: ['2025-09-01', '2025-09-02', '2025-09-03', '2025-09-04', '2025-09-05'],
    datasets: [
      {
        label: 'Sample Data',
        data: [0, 0, 0, 0, 0],
        color: '#8884d8'
      }
    ]
  });

  const getMockBarData = () => ({
    labels: ['No Data'],
    datasets: [
      {
        label: 'No Data Available',
        data: [0],
        color: '#cccccc'
      }
    ]
  });

  const getMockPieData = () => ({
    labels: ['No Data Available'],
    values: [1],
    colors: ['#cccccc']
  });

  const fetchJobsOverview = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/reports/jobs-overview');
      if (response.ok) {
        const data = await response.json();
        setJobsOverview(data);
      } else {
        console.error('Failed to fetch jobs overview:', response.status, response.statusText);
        // Set fallback data
        setJobsOverview({
          jobs: [],
          period_days: 30
        });
        toast({
          title: "Warning",
          description: "Unable to fetch latest data, showing offline mode",
          variant: "destructive",
        });
      }
    } catch (error) {
      console.error('Error fetching jobs overview:', error);
      // Set fallback data
      setJobsOverview({
        jobs: [],
        period_days: 30
      });
      toast({
        title: "Warning", 
        description: "Network error, showing offline mode",
        variant: "destructive",
      });
    }
  };

  const fetchDashboardStats = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/reports/dashboard-stats');
      if (response.ok) {
        const data = await response.json();
        setDashboardStats(data);
      } else {
        console.error('Failed to fetch dashboard stats:', response.status, response.statusText);
        // Set fallback data
        setDashboardStats({
          job_statistics: [],
          quality_trends: [],
          error_distribution: [],
          summary: {
            total_jobs: 0,
            avg_quality_score: 0,
            total_errors: 0
          }
        });
        toast({
          title: "Warning",
          description: "Unable to fetch dashboard statistics",
          variant: "destructive",
        });
      }
    } catch (error) {
      console.error('Error fetching dashboard stats:', error);
      // Set fallback data  
      setDashboardStats({
        job_statistics: [],
        quality_trends: [],
        error_distribution: [],
        summary: {
          total_jobs: 0,
          avg_quality_score: 0,
          total_errors: 0
        }
      });
      toast({
        title: "Warning", 
        description: "Network error, showing offline mode",
        variant: "destructive",
      });
    }
  };

  const fetchJobReport = async (jobId: string) => {
    try {
      const response = await fetch(`http://localhost:8000/api/reports/${jobId}`);
      if (response.ok) {
        const data = await response.json();
        setSelectedJobReport(data);
        setIsReportModalOpen(true);
      } else {
        console.error('Failed to fetch job report:', response.status, response.statusText);
        toast({
          title: "Error",
          description: "Failed to fetch job report",
          variant: "destructive",
        });
      }
    } catch (error) {
      console.error('Error fetching job report:', error);
      toast({
        title: "Error",
        description: "Network error fetching job report", 
        variant: "destructive",
      });
    }
  };

  const fetchAllData = async () => {
    setLoading(true);
    await Promise.all([
      fetchJobsOverview(),
      fetchDashboardStats()
    ]);
    setLoading(false);
  };

  useEffect(() => {
    fetchAllData();
  }, []);

  const getStatusBadge = (status: string) => {
    switch (status.toLowerCase()) {
      case 'completed':
        return <Badge className="bg-green-100 text-green-800">Completed</Badge>;
      case 'failed':
        return <Badge className="bg-red-100 text-red-800">Failed</Badge>;
      case 'processing':
        return <Badge className="bg-blue-100 text-blue-800">Processing</Badge>;
      case 'pending':
        return <Badge className="bg-yellow-100 text-yellow-800">Pending</Badge>;
      default:
        return <Badge className="bg-gray-100 text-gray-800">{status}</Badge>;
    }
  };

  const getSeverityColor = (percentage: number) => {
    if (percentage >= 90) return 'text-green-600';
    if (percentage >= 75) return 'text-yellow-600';
    return 'text-red-600';
  };

  const formatDate = (dateString?: string) => {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleString();
  };

  if (loading) {
    return (
      <div className="container mx-auto p-6">
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-gray-900"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Data Pipeline Reports</h1>
          <p className="text-gray-500 mt-1">Analysis of data processing operations</p>
        </div>
        <Button 
          onClick={fetchAllData}
          className="flex items-center gap-2"
        >
          <RefreshCw className="h-4 w-4" />
          Refresh
        </Button>
      </div>

      <Tabs defaultValue="dashboard" className="space-y-4">
        <TabsList>
          <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
          <TabsTrigger value="uploads">Recent Uploads</TabsTrigger>
          <TabsTrigger value="metrics">Quality Metrics</TabsTrigger>
          <TabsTrigger value="rules">Validation Rules</TabsTrigger>
        </TabsList>

        <TabsContent value="dashboard" className="space-y-4">
          {dashboardStats ? (
            <>
              {/* Summary Cards */}
              <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Total Jobs</CardTitle>
                    <Database className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{dashboardStats.summary.total_jobs}</div>
                    <p className="text-xs text-muted-foreground">
                      {dashboardStats.job_statistics.find(s => s.status === 'completed')?.count || 0} completed
                    </p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Average Quality</CardTitle>
                    <TrendingUp className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{dashboardStats.summary.avg_quality_score.toFixed(1)}%</div>
                    <p className="text-xs text-muted-foreground">Last 30 days</p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Total Errors</CardTitle>
                    <AlertTriangle className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">{dashboardStats.summary.total_errors}</div>
                    <p className="text-xs text-muted-foreground">Across all jobs</p>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">Total Records</CardTitle>
                    <Activity className="h-4 w-4 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl font-bold">
                      {jobsOverview?.jobs.reduce((sum, job) => sum + job.total_records, 0).toLocaleString() || 0}
                    </div>
                    <p className="text-xs text-muted-foreground">Processed</p>
                  </CardContent>
                </Card>
              </div>

              {/* Charts Row 1 */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <PieChartIcon className="h-5 w-5" />
                      Job Status Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <PieChart 
                      data={dashboardStats?.job_statistics ? {
                        labels: dashboardStats.job_statistics.map(stat => stat.status),
                        values: dashboardStats.job_statistics.map(stat => stat.count),
                        colors: ['#8884d8', '#82ca9d', '#ffc658', '#ff7c7c']
                      } : getMockPieData()}
                    />
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <BarChart3 className="h-5 w-5" />
                      Error Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <BarChart 
                      data={dashboardStats?.error_distribution ? {
                        labels: dashboardStats.error_distribution.map(item => item.error_type),
                        datasets: [{
                          label: 'Error Count',
                          data: dashboardStats.error_distribution.map(item => item.count),
                          color: '#ff7c7c'
                        }]
                      } : getMockBarData()}
                    />
                  </CardContent>
                </Card>
              </div>

              {/* Quality Trends Chart */}
              <Card>
                <CardHeader>
                  <CardTitle>Data Quality Trends</CardTitle>
                  <CardDescription>Quality metrics over time</CardDescription>
                </CardHeader>
                <CardContent>
                  <TimeSeriesChart 
                    data={dashboardStats?.quality_trends ? {
                      timestamps: dashboardStats.quality_trends.map(item => item.date),
                      datasets: [
                        {
                          label: 'Validity',
                          data: dashboardStats.quality_trends.map(item => item.validity),
                          color: '#8884d8'
                        },
                        {
                          label: 'Completeness',
                          data: dashboardStats.quality_trends.map(item => item.completeness),
                          color: '#82ca9d'
                        },
                        {
                          label: 'Consistency',
                          data: dashboardStats.quality_trends.map(item => item.consistency),
                          color: '#ffc658'
                        }
                      ]
                    } : getMockTimeSeriesData()}
                  />
                </CardContent>
              </Card>

              {/* Missing Values Analysis Chart */}
              {jobsOverview?.jobs && (
                <Card>
                  <CardHeader>
                    <CardTitle>Missing Values Analysis</CardTitle>
                    <CardDescription>Missing data percentage by job</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <BarChart 
                      data={jobsOverview?.jobs ? {
                        labels: jobsOverview.jobs
                          .filter(job => job.missing_pct > 0)
                          .slice(0, 10)
                          .map(job => job.job_name.length > 20 ? job.job_name.substring(0, 20) + '...' : job.job_name),
                        datasets: [{
                          label: 'Missing %',
                          data: jobsOverview.jobs
                            .filter(job => job.missing_pct > 0)
                            .slice(0, 10)
                            .map(job => job.missing_pct),
                          color: '#ff7c7c'
                        }]
                      } : getMockBarData()}
                    />
                  </CardContent>
                </Card>
              )}

              {/* Data Quality Breakdown Chart */}
              {jobsOverview?.jobs && (
                <Card>
                  <CardHeader>
                    <CardTitle>Data Quality Breakdown</CardTitle>
                    <CardDescription>Success rate distribution across jobs</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <BarChart 
                      data={jobsOverview?.jobs ? {
                        labels: jobsOverview.jobs
                          .slice(0, 10)
                          .map(job => job.job_name.length > 15 ? job.job_name.substring(0, 15) + '...' : job.job_name),
                        datasets: [{
                          label: 'Success Rate %',
                          data: jobsOverview.jobs
                            .slice(0, 10)
                            .map(job => job.success_rate_pct),
                          color: '#82ca9d'
                        }]
                      } : getMockBarData()}
                    />
                  </CardContent>
                </Card>
              )}
            </>
          ) : (
            <Card>
              <CardContent className="text-center py-8">
                <p className="text-gray-500">No dashboard data available</p>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        <TabsContent value="uploads" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <FileText className="h-5 w-5" />
                Recent Data Uploads
              </CardTitle>
              <CardDescription>
                Overview of recent data processing jobs with quality metrics
              </CardDescription>
            </CardHeader>
            <CardContent>
              {jobsOverview?.jobs && jobsOverview.jobs.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="w-full border-collapse border border-gray-200">
                    <thead>
                      <tr className="bg-gray-50">
                        <th className="border border-gray-200 px-4 py-2 text-left">File Name</th>
                        <th className="border border-gray-200 px-4 py-2 text-left">Status</th>
                        <th className="border border-gray-200 px-4 py-2 text-right">Total Records</th>
                        <th className="border border-gray-200 px-4 py-2 text-right">Success Rate</th>
                        <th className="border border-gray-200 px-4 py-2 text-right">Invalid %</th>
                        <th className="border border-gray-200 px-4 py-2 text-right">Missing %</th>
                        <th className="border border-gray-200 px-4 py-2 text-left">Created</th>
                        <th className="border border-gray-200 px-4 py-2 text-center">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {jobsOverview.jobs.map((job) => (
                        <tr key={job.job_id} className="hover:bg-gray-50">
                          <td className="border border-gray-200 px-4 py-2 font-medium">
                            {job.job_name}
                          </td>
                          <td className="border border-gray-200 px-4 py-2">
                            {getStatusBadge(job.status)}
                          </td>
                          <td className="border border-gray-200 px-4 py-2 text-right">
                            {job.total_records.toLocaleString()}
                          </td>
                          <td className={`border border-gray-200 px-4 py-2 text-right font-semibold ${getSeverityColor(job.success_rate_pct)}`}>
                            {job.success_rate_pct.toFixed(1)}%
                          </td>
                          <td className={`border border-gray-200 px-4 py-2 text-right ${job.invalid_pct > 5 ? 'text-red-600' : 'text-gray-600'}`}>
                            {job.invalid_pct.toFixed(1)}%
                          </td>
                          <td className={`border border-gray-200 px-4 py-2 text-right ${job.missing_pct > 10 ? 'text-orange-600' : 'text-gray-600'}`}>
                            {job.missing_pct.toFixed(1)}%
                          </td>
                          <td className="border border-gray-200 px-4 py-2 text-sm text-gray-500">
                            {formatDate(job.created_at)}
                          </td>
                          <td className="border border-gray-200 px-4 py-2 text-center">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => fetchJobReport(job.job_id)}
                              className="flex items-center gap-1"
                            >
                              <Eye className="h-3 w-3" />
                              View Report
                            </Button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-center py-8 text-gray-500">
                  <FileText className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                  <p>No recent uploads found</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="metrics" className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Total Jobs</CardTitle>
                <Database className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{jobsOverview?.jobs.length || 0}</div>
                <p className="text-xs text-muted-foreground">Last 30 days</p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Average Success Rate</CardTitle>
                <TrendingUp className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {jobsOverview?.jobs.length 
                    ? (jobsOverview.jobs.reduce((sum, job) => sum + job.success_rate_pct, 0) / jobsOverview.jobs.length).toFixed(1)
                    : 0}%
                </div>
                <p className="text-xs text-muted-foreground">All completed jobs</p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Total Records Processed</CardTitle>
                <Activity className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {jobsOverview?.jobs.reduce((sum, job) => sum + job.total_records, 0).toLocaleString() || 0}
                </div>
                <p className="text-xs text-muted-foreground">All time</p>
              </CardContent>
            </Card>
          </div>

          {/* Missing Values Analysis */}
          <Card>
            <CardHeader>
              <CardTitle>Missing Values Analysis</CardTitle>
              <CardDescription>Missing data percentage by upload</CardDescription>
            </CardHeader>
            <CardContent>
              {jobsOverview?.jobs && jobsOverview.jobs.length > 0 ? (
                <div className="space-y-3">
                  {jobsOverview.jobs
                    .filter(job => job.missing_pct > 0)
                    .map((job) => (
                      <div key={job.job_id} className="flex items-center justify-between p-3 border rounded-lg">
                        <div>
                          <p className="font-medium">{job.job_name}</p>
                          <p className="text-sm text-gray-500">{job.total_records.toLocaleString()} total records</p>
                        </div>
                        <div className="text-right">
                          <p className={`text-lg font-bold ${job.missing_pct > 10 ? 'text-red-600' : job.missing_pct > 5 ? 'text-orange-600' : 'text-yellow-600'}`}>
                            {job.missing_pct.toFixed(1)}%
                          </p>
                          <p className="text-xs text-gray-500">missing values</p>
                        </div>
                      </div>
                    ))}
                  {jobsOverview.jobs.filter(job => job.missing_pct > 0).length === 0 && (
                    <div className="text-center py-4 text-gray-500">
                      <p>No missing values detected in recent uploads</p>
                    </div>
                  )}
                </div>
              ) : (
                <div className="text-center py-8 text-gray-500">
                  <p>No data available for analysis</p>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Data Quality Distribution */}
          <Card>
            <CardHeader>
              <CardTitle>Data Quality Distribution</CardTitle>
              <CardDescription>Quality scores across all uploads</CardDescription>
            </CardHeader>
            <CardContent>
              {jobsOverview?.jobs && jobsOverview.jobs.length > 0 ? (
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className="text-center p-4 border rounded-lg bg-green-50">
                    <p className="text-2xl font-bold text-green-600">
                      {jobsOverview.jobs.filter(job => job.success_rate_pct >= 90).length}
                    </p>
                    <p className="text-sm text-green-700">High Quality (≥90%)</p>
                  </div>
                  <div className="text-center p-4 border rounded-lg bg-yellow-50">
                    <p className="text-2xl font-bold text-yellow-600">
                      {jobsOverview.jobs.filter(job => job.success_rate_pct >= 75 && job.success_rate_pct < 90).length}
                    </p>
                    <p className="text-sm text-yellow-700">Medium Quality (75-89%)</p>
                  </div>
                  <div className="text-center p-4 border rounded-lg bg-red-50">
                    <p className="text-2xl font-bold text-red-600">
                      {jobsOverview.jobs.filter(job => job.success_rate_pct < 75).length}
                    </p>
                    <p className="text-sm text-red-700">Low Quality (&lt;75%)</p>
                  </div>
                </div>
              ) : (
                <div className="text-center py-8 text-gray-500">
                  <p>No quality data available</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="rules" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Validation Rules Overview</CardTitle>
              <CardDescription>
                Summary of validation rule performance across all jobs
              </CardDescription>
            </CardHeader>
            <CardContent>
              {jobsOverview?.jobs && jobsOverview.jobs.length > 0 ? (
                <div className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="p-4 border rounded-lg">
                      <h4 className="font-medium mb-2">Invalid Records Analysis</h4>
                      <div className="space-y-2">
                        {jobsOverview.jobs
                          .filter(job => job.invalid_records > 0)
                          .slice(0, 5)
                          .map(job => (
                            <div key={job.job_id} className="flex justify-between">
                              <span className="text-sm truncate">{job.job_name}</span>
                              <span className="text-sm text-red-600">{job.invalid_records.toLocaleString()} invalid</span>
                            </div>
                          ))}
                      </div>
                    </div>
                    
                    <div className="p-4 border rounded-lg">
                      <h4 className="font-medium mb-2">Success Rate Distribution</h4>
                      <div className="space-y-2">
                        <div className="flex justify-between">
                          <span className="text-sm">Perfect (100%)</span>
                          <span className="text-sm font-semibold">{jobsOverview.jobs.filter(j => j.success_rate_pct === 100).length}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm">High (95-99%)</span>
                          <span className="text-sm font-semibold">{jobsOverview.jobs.filter(j => j.success_rate_pct >= 95 && j.success_rate_pct < 100).length}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm">Good (90-94%)</span>
                          <span className="text-sm font-semibold">{jobsOverview.jobs.filter(j => j.success_rate_pct >= 90 && j.success_rate_pct < 95).length}</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm">Needs Review (&lt;90%)</span>
                          <span className="text-sm font-semibold text-red-600">{jobsOverview.jobs.filter(j => j.success_rate_pct < 90).length}</span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="text-center py-8 text-gray-500">
                  <AlertTriangle className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                  <p>No validation data available</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Job Report Modal */}
      <Dialog open={isReportModalOpen} onOpenChange={setIsReportModalOpen}>
        <DialogContent className="max-w-6xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center justify-between">
              <span>Job Report: {selectedJobReport?.job_info.job_name}</span>
              <Button 
                variant="ghost" 
                size="sm" 
                onClick={() => setIsReportModalOpen(false)}
                className="h-6 w-6 p-0"
              >
                <X className="h-4 w-4" />
              </Button>
            </DialogTitle>
            <DialogDescription>
              Detailed validation results and quality metrics
            </DialogDescription>
          </DialogHeader>
          
          {selectedJobReport && (
            <div className="space-y-6">
              {/* Job Information */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Job Information</CardTitle>
                </CardHeader>
                <CardContent className="grid grid-cols-2 md:grid-cols-3 gap-4">
                  <div>
                    <p className="text-sm font-medium text-gray-500">Job ID</p>
                    <p className="text-sm font-mono break-all">{selectedJobReport.job_info.job_id}</p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-500">Status</p>
                    <p className="text-sm">{getStatusBadge(selectedJobReport.job_info.status)}</p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-500">Source Type</p>
                    <p className="text-sm">{selectedJobReport.job_info.source_type || 'N/A'}</p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-500">Format</p>
                    <p className="text-sm">{selectedJobReport.job_info.source_format || 'N/A'}</p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-500">Created At</p>
                    <p className="text-sm">{formatDate(selectedJobReport.job_info.created_at)}</p>
                  </div>
                  <div>
                    <p className="text-sm font-medium text-gray-500">Duration</p>
                    <p className="text-sm">
                      {selectedJobReport.job_info.started_at && selectedJobReport.job_info.completed_at
                        ? `${Math.round((new Date(selectedJobReport.job_info.completed_at).getTime() - new Date(selectedJobReport.job_info.started_at).getTime()) / 1000)}s`
                        : 'N/A'}
                    </p>
                  </div>
                </CardContent>
              </Card>

              {/* Processing Statistics */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Processing Statistics</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                    <div className="text-center p-4 border rounded-lg bg-blue-50">
                      <p className="text-2xl font-bold text-blue-600">{selectedJobReport.statistics.total_records.toLocaleString()}</p>
                      <p className="text-sm text-blue-700">Total Records</p>
                    </div>
                    <div className="text-center p-4 border rounded-lg bg-green-50">
                      <p className="text-2xl font-bold text-green-600">{selectedJobReport.statistics.valid_records.toLocaleString()}</p>
                      <p className="text-sm text-green-700">Valid Records</p>
                    </div>
                    <div className="text-center p-4 border rounded-lg bg-red-50">
                      <p className="text-2xl font-bold text-red-600">{selectedJobReport.statistics.invalid_records.toLocaleString()}</p>
                      <p className="text-sm text-red-700">Invalid Records</p>
                    </div>
                    <div className="text-center p-4 border rounded-lg bg-orange-50">
                      <p className="text-2xl font-bold text-orange-600">{selectedJobReport.statistics.quarantined_records.toLocaleString()}</p>
                      <p className="text-sm text-orange-700">Quarantined</p>
                    </div>
                    <div className="text-center p-4 border rounded-lg bg-purple-50">
                      <p className="text-2xl font-bold text-purple-600">{selectedJobReport.statistics.success_rate.toFixed(1)}%</p>
                      <p className="text-sm text-purple-700">Success Rate</p>
                    </div>
                    <div className="text-center p-4 border rounded-lg bg-indigo-50">
                      <p className="text-2xl font-bold text-indigo-600">{(selectedJobReport.statistics.avg_quality_score * 100).toFixed(1)}%</p>
                      <p className="text-sm text-indigo-700">Quality Score</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              {/* Validation Results */}
              {selectedJobReport.validation_results.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">Validation Results</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="overflow-x-auto">
                      <table className="w-full border-collapse border border-gray-200">
                        <thead>
                          <tr className="bg-gray-50">
                            <th className="border border-gray-200 px-3 py-2 text-left">Rule Name</th>
                            <th className="border border-gray-200 px-3 py-2 text-left">Field</th>
                            <th className="border border-gray-200 px-3 py-2 text-right">Checked</th>
                            <th className="border border-gray-200 px-3 py-2 text-right">Passed</th>
                            <th className="border border-gray-200 px-3 py-2 text-right">Failed</th>
                            <th className="border border-gray-200 px-3 py-2 text-right">Success Rate</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedJobReport.validation_results.map((result, index) => (
                            <tr key={result.result_id || index} className="hover:bg-gray-50">
                              <td className="border border-gray-200 px-3 py-2 font-medium">{result.rule_name}</td>
                              <td className="border border-gray-200 px-3 py-2">{result.field_name}</td>
                              <td className="border border-gray-200 px-3 py-2 text-right">{result.records_checked.toLocaleString()}</td>
                              <td className="border border-gray-200 px-3 py-2 text-right text-green-600">{result.records_passed.toLocaleString()}</td>
                              <td className="border border-gray-200 px-3 py-2 text-right text-red-600">{result.records_failed.toLocaleString()}</td>
                              <td className={`border border-gray-200 px-3 py-2 text-right font-semibold ${getSeverityColor(result.success_rate)}`}>
                                {result.success_rate.toFixed(1)}%
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Quality Metrics */}
              {selectedJobReport.quality_metrics.length > 0 && (
                <Card>
                  <CardHeader>
                    <CardTitle className="text-lg">Quality Metrics</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                      {selectedJobReport.quality_metrics.map((metric, index) => (
                        <div key={metric.metric_id || index} className="p-4 border rounded-lg bg-gray-50">
                          <p className="font-medium">{metric.metric_name}</p>
                          <p className="text-sm text-gray-500 mb-2">{metric.field_name}</p>
                          <p className="text-lg font-bold">{metric.metric_value.toFixed(2)}</p>
                          <p className="text-xs text-gray-400">{metric.metric_type}</p>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Raw JSON Data */}
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Raw Report Data (JSON)</CardTitle>
                </CardHeader>
                <CardContent>
                  <pre className="bg-gray-100 p-4 rounded-lg text-xs overflow-auto max-h-96">
                    {JSON.stringify(selectedJobReport, null, 2)}
                  </pre>
                </CardContent>
              </Card>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default Reports;