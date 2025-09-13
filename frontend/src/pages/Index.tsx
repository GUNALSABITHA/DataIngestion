import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Link } from "react-router-dom";
import { Upload, Database, Shield, BarChart3, CheckCircle, Clock, FileText, AlertCircle, Loader2 } from "lucide-react";
import { useState, useEffect } from "react";

const Index = () => {
  const [recentValidations, setRecentValidations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch recent validations from API
  useEffect(() => {
    const fetchRecentValidations = async () => {
      try {
        setLoading(true);
        setError(null);
        
        const response = await fetch('http://localhost:8000/api/recent-validations?limit=5');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        setRecentValidations(data.validations || []);
      } catch (err) {
        console.error('Failed to fetch recent validations:', err);
        setError('Failed to load recent validations');
        // Set empty array instead of fallback mock data
        setRecentValidations([]);
      } finally {
        setLoading(false);
      }
    };

    fetchRecentValidations();
  }, []);

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-4 w-4 text-success" />;
      case 'error':
      case 'failed':
        return <AlertCircle className="h-4 w-4 text-destructive" />;
      case 'running':
        return <Loader2 className="h-4 w-4 text-primary animate-spin" />;
      default:
        return <Clock className="h-4 w-4 text-muted-foreground" />;
    }
  };

  const getStatusVariant = (status: string) => {
    switch (status) {
      case 'completed':
        return 'default';
      case 'error':
      case 'failed':
        return 'destructive';
      case 'running':
        return 'secondary';
      default:
        return 'outline';
    }
  };

  const features = [
    {
      icon: Upload,
      title: "Easy Upload",
      description: "Drag and drop your datasets for instant validation",
    },
    {
      icon: Shield,
      title: "Data Security",
      description: "Enterprise-grade security with quarantine protection",
    },
    {
      icon: BarChart3,
      title: "Quality Analytics",
      description: "Comprehensive quality scoring and insights",
    },
    {
      icon: Database,
      title: "Multiple Formats",
      description: "Support for CSV, JSON, Excel, and more",
    },
  ];

  return (
    <div className="container mx-auto px-4 py-12">
      <div className="max-w-6xl mx-auto">
        {/* Hero Section */}
        <div className="text-center mb-16">
          <h1 className="text-5xl font-bold text-foreground mb-6">
            Data Validation <span className="text-primary">Framework</span>
          </h1>
          <p className="text-xl text-muted-foreground mb-8 max-w-3xl mx-auto">
            Ensure data quality and integrity with our comprehensive validation platform. 
            Upload, analyze, and monitor your datasets with enterprise-grade security and analytics.
          </p>
          
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link to="/upload">
              <Button size="lg" className="text-lg px-8 py-6">
                <Upload className="mr-2 h-5 w-5" />
                Upload 
              </Button>
            </Link>
            <Link to="/results">
              <Button variant="outline" size="lg" className="text-lg px-8 py-6">
                <FileText className="mr-2 h-5 w-5" />
                Previous Validations
              </Button>
            </Link>
          </div>
        </div>

        {/* Features Grid */}
        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
          {features.map((feature, index) => (
            <Card key={index} className="text-center hover:shadow-lg transition-shadow">
              <CardHeader>
                <div className="mx-auto w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center mb-4">
                  <feature.icon className="h-6 w-6 text-primary" />
                </div>
                <CardTitle className="text-lg">{feature.title}</CardTitle>
              </CardHeader>
              <CardContent>
                <CardDescription>{feature.description}</CardDescription>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Quick Stats & Recent Activity */}
        <div className="grid lg:grid-cols-3 gap-8">
          <div className="lg:col-span-2">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="flex items-center space-x-2">
                    <Clock className="h-5 w-5" />
                    <span>Recent Validations</span>
                  </CardTitle>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setLoading(true);
                      setError(null);
                      // Re-trigger the useEffect by changing the dependency
                      const fetchRecentValidations = async () => {
                        try {
                          const response = await fetch('http://localhost:8000/api/recent-validations?limit=5');
                          if (!response.ok) {
                            throw new Error(`HTTP error! status: ${response.status}`);
                          }
                          const data = await response.json();
                          setRecentValidations(data.validations || []);
                        } catch (err) {
                          console.error('Failed to fetch recent validations:', err);
                          setError('Failed to load recent validations');
                          // Keep current fallback data if any
                        } finally {
                          setLoading(false);
                        }
                      };
                      fetchRecentValidations();
                    }}
                    disabled={loading}
                  >
                    {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : "Refresh"}
                  </Button>
                </div>
                <CardDescription>
                  Your latest data validation activity
                </CardDescription>
              </CardHeader>
              <CardContent>
                {loading ? (
                  <div className="flex items-center justify-center py-8">
                    <Loader2 className="h-6 w-6 animate-spin text-primary" />
                    <span className="ml-2 text-muted-foreground">Loading recent validations...</span>
                  </div>
                ) : error ? (
                  <div className="flex items-center justify-center py-8 text-muted-foreground">
                    <AlertCircle className="h-5 w-5 mr-2" />
                    <span>{error}</span>
                  </div>
                ) : recentValidations.length === 0 ? (
                  <div className="flex items-center justify-center py-8 text-muted-foreground">
                    <Clock className="h-5 w-5 mr-2" />
                    <span>No recent validations found</span>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {recentValidations.map((validation, index) => (
                      <div key={validation.id || index} className="flex items-center justify-between p-3 bg-muted/30 rounded-lg">
                        <div className="flex items-center space-x-3">
                          {getStatusIcon(validation.status)}
                          <span className="font-medium">{validation.name}</span>
                        </div>
                        <div className="flex items-center space-x-2">
                          <Badge variant={getStatusVariant(validation.status)}>
                            {validation.status}
                          </Badge>
                          <span className="text-sm text-muted-foreground">{validation.timestamp}</span>
                        </div>
                      </div>
                    ))}
                    {recentValidations.length > 0 && (
                      <div className="pt-2 border-t">
                        <Link to="/results">
                          <Button variant="ghost" size="sm" className="w-full">
                            View All Validations →
                          </Button>
                        </Link>
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Data Quality Score</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-3xl font-bold text-primary mb-2">94.2%</div>
                <p className="text-sm text-muted-foreground">
                  Average across all datasets
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Total Datasets</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-3xl font-bold text-foreground mb-2">1,247</div>
                <p className="text-sm text-muted-foreground">
                  Successfully validated
                </p>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Index;
