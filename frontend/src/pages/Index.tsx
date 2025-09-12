import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Link } from "react-router-dom";
import { Upload, Database, Shield, BarChart3, CheckCircle, Clock, FileText } from "lucide-react";

const Index = () => {
  const recentValidations = [
    { name: "customer_data.csv", status: "completed", timestamp: "2 hours ago" },
    { name: "product_inventory.xlsx", status: "warning", timestamp: "4 hours ago" },
    { name: "transaction_log.json", status: "completed", timestamp: "1 day ago" },
  ];

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
                <CardTitle className="flex items-center space-x-2">
                  <Clock className="h-5 w-5" />
                  <span>Recent Validations</span>
                </CardTitle>
                <CardDescription>
                  Your latest data validation activity
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {recentValidations.map((validation, index) => (
                    <div key={index} className="flex items-center justify-between p-3 bg-muted/30 rounded-lg">
                      <div className="flex items-center space-x-3">
                        <CheckCircle className="h-4 w-4 text-success" />
                        <span className="font-medium">{validation.name}</span>
                      </div>
                      <div className="flex items-center space-x-2">
                        <Badge variant={validation.status === "completed" ? "default" : "secondary"}>
                          {validation.status}
                        </Badge>
                        <span className="text-sm text-muted-foreground">{validation.timestamp}</span>
                      </div>
                    </div>
                  ))}
                </div>
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
