import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { 
  TrendingUp, 
  Activity, 
  Database, 
  RefreshCw,
  Eye,
  FileText,
  AlertTriangle
} from "lucide-react";
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

const Reports = () => {
  const [jobsOverview, setJobsOverview] = useState<JobsOverview | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchJobsOverview = async () => {
    try {
      setLoading(true);
      const response = await fetch('http://localhost:8000/api/reports/jobs-overview?days=30');
      if (response.ok) {
        const data = await response.json();
        setJobsOverview(data);
      } else {
        console.error('Failed to fetch jobs overview:', response.status, response.statusText);
        toast({
          title: "Error",
          description: "Failed to fetch jobs overview",
          variant: "destructive",
        });
      }
    } catch (error) {
      console.error('Error fetching jobs overview:', error);
      toast({
        title: "Error", 
        description: "Network error fetching jobs overview",
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  };

  const viewJobReport = (jobId: string) => {
    // Open report in new window
    window.open(`http://localhost:8000/api/reports/${jobId}`, '_blank');
  };

  useEffect(() => {
    fetchJobsOverview();
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
          onClick={fetchJobsOverview}
          className="flex items-center gap-2"
        >
          <RefreshCw className="h-4 w-4" />
          Refresh
        </Button>
      </div>

      <Tabs defaultValue="uploads" className="space-y-4">
        <TabsList>
          <TabsTrigger value="uploads">Recent Uploads</TabsTrigger>
          <TabsTrigger value="metrics">Quality Metrics</TabsTrigger>
          <TabsTrigger value="rules">Validation Rules</TabsTrigger>
        </TabsList>

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
                              onClick={() => viewJobReport(job.job_id)}
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
    </div>
  );
};

export default Reports;