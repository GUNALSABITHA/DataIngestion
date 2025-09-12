import React, { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Upload as UploadIcon, FileText, Database, Loader2, Globe, Link } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { apiClient, JobStatus } from "@/lib/api";
import { useNavigate } from "react-router-dom";

const Upload = () => {
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [apiUrl, setApiUrl] = useState("");
  const [streamingData, setStreamingData] = useState(false);
  const { toast } = useToast();
  const navigate = useNavigate();
  const fileInputRef = React.useRef<HTMLInputElement>(null);

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragleave" || e.type === "dragover") {
      setDragActive(e.type !== "dragleave");
    }
  };

  async function uploadSingle(file: File) {
    setUploadProgress(0);

    toast({
      title: "Uploading file...",
      description: `Processing ${file.name}`,
    });

    // Upload file to backend
    const response = await apiClient.uploadFile(file, "validate");

    toast({
      title: "File uploaded",
      description: `Job ID: ${response.job_id}`,
    });

    // Poll for job completion
    await apiClient.pollJobStatus(response.job_id, (status: JobStatus) => {
      setUploadProgress(status.progress);
    });

    toast({
      title: "Validation completed",
      description: `${file.name} processed successfully`,
    });
  }

  const handleFileUpload = async (file: File) => {
    if (uploading) return;
    try {
      setUploading(true);
      await uploadSingle(file);
      navigate("/results");
    } catch (error) {
      console.error("Upload error:", error);
      toast({
        title: "Upload failed",
        description: error instanceof Error ? error.message : "An error occurred",
        variant: "destructive",
      });
    } finally {
      setUploading(false);
      setUploadProgress(0);
    }
  };

  const handleFilesUpload = async (files: File[]) => {
    if (!files.length || uploading) return;
    setUploading(true);
    try {
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        toast({
          title: `Uploading ${i + 1}/${files.length}`,
          description: file.name,
        });
        await uploadSingle(file);
      }
      navigate("/results");
    } catch (error) {
      console.error("Batch upload error:", error);
      toast({
        title: "Batch upload failed",
        description: error instanceof Error ? error.message : "An error occurred",
        variant: "destructive",
      });
    } finally {
      setUploading(false);
      setUploadProgress(0);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    const files = Array.from(e.dataTransfer.files || []);
    if (files.length > 0) {
      handleFilesUpload(files);
    }
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (files.length > 0) {
      handleFilesUpload(files);
    }
    // Reset the input so the same file can be selected again
    e.target.value = "";
  };

  const handleButtonClick = () => {
    if (fileInputRef.current && !uploading) {
      fileInputRef.current.click();
    }
  };

  const handleApiStreaming = async () => {
    if (!apiUrl.trim() || uploading) return;
    
    try {
      setUploading(true);
      setStreamingData(true);
      setUploadProgress(0);

      toast({
        title: "Starting API streaming...",
        description: `Connecting to ${apiUrl}`,
      });

      // Start API streaming job
      const response = await apiClient.startApiStreaming(apiUrl, "validate");

      toast({
        title: "API streaming started",
        description: `Job ID: ${response.job_id}`,
      });

      // Poll for job completion
      await apiClient.pollJobStatus(response.job_id, (status: JobStatus) => {
        setUploadProgress(status.progress);
      });

      toast({
        title: "Streaming completed",
        description: `Data from ${apiUrl} processed successfully`,
      });

      navigate("/results");
    } catch (error) {
      console.error("API streaming error:", error);
      toast({
        title: "API streaming failed",
        description: error instanceof Error ? error.message : "An error occurred",
        variant: "destructive",
      });
    } finally {
      setUploading(false);
      setStreamingData(false);
      setUploadProgress(0);
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-foreground mb-2">Data Ingestion</h1>
          <p className="text-muted-foreground">
            Upload files or stream data from APIs for validation and quality analysis
          </p>
        </div>

        <Card className="mb-8">
          <CardContent className="p-8">
            <Tabs defaultValue="file-upload" className="w-full">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="file-upload" className="flex items-center space-x-2">
                  <UploadIcon className="h-4 w-4" />
                  <span>File Upload</span>
                </TabsTrigger>
                <TabsTrigger value="api-streaming" className="flex items-center space-x-2">
                  <Globe className="h-4 w-4" />
                  <span>API Streaming</span>
                </TabsTrigger>
              </TabsList>
              
              <TabsContent value="file-upload" className="mt-6">
                <div
                  className={`border-2 border-dashed rounded-lg p-12 text-center transition-colors ${
                    dragActive
                      ? "border-primary bg-primary/5"
                      : uploading && !streamingData
                      ? "border-muted bg-muted/50"
                      : "border-border hover:border-primary/50"
                  }`}
                  onDragEnter={handleDrag}
                  onDragLeave={handleDrag}
                  onDragOver={handleDrag}
                  onDrop={handleDrop}
                >
                  {uploading && !streamingData ? (
                    <>
                      <Loader2 className="h-16 w-16 text-primary mx-auto mb-4 animate-spin" />
                      <h3 className="text-xl font-semibold mb-2">Processing your file...</h3>
                      <p className="text-muted-foreground mb-4">{uploadProgress}% complete</p>
                      <div className="w-full bg-secondary rounded-full h-2 mb-4">
                        <div
                          className="bg-primary h-2 rounded-full transition-all duration-300"
                          style={{ width: `${uploadProgress}%` }}
                        ></div>
                      </div>
                    </>
                  ) : (
                    <>
                      <UploadIcon className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
                      <h3 className="text-xl font-semibold mb-2">Drag and drop your files here</h3>
                      <p className="text-muted-foreground mb-4">or click to browse your computer</p>
                      <input
                        ref={fileInputRef}
                        type="file"
                        multiple
                        accept=".csv,.json,.xlsx,.xls"
                        onChange={handleFileInput}
                        className="hidden"
                        disabled={uploading}
                      />
                      <Button
                        variant="default"
                        className="cursor-pointer"
                        disabled={uploading}
                        onClick={handleButtonClick}
                      >
                        Choose Files
                      </Button>
                      <p className="text-sm text-muted-foreground mt-4">Supported formats: CSV, JSON, Excel</p>
                    </>
                  )}
                </div>
              </TabsContent>
              
              <TabsContent value="api-streaming" className="mt-6">
                <div className="space-y-6">
                  <div className="text-center">
                    <Globe className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
                    <h3 className="text-xl font-semibold mb-2">Stream Data from API</h3>
                    <p className="text-muted-foreground mb-6">
                      Enter an API endpoint URL to stream and validate data in real-time
                    </p>
                  </div>
                  
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <Label htmlFor="api-url">API Endpoint URL</Label>
                      <div className="flex space-x-2">
                        <Input
                          id="api-url"
                          type="url"
                          placeholder="https://api.example.com/data"
                          value={apiUrl}
                          onChange={(e) => setApiUrl(e.target.value)}
                          disabled={uploading}
                          className="flex-1"
                        />
                        <Button
                          onClick={handleApiStreaming}
                          disabled={!apiUrl.trim() || uploading}
                          className="px-6"
                        >
                          {uploading && streamingData ? (
                            <>
                              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                              Streaming...
                            </>
                          ) : (
                            <>
                              <Link className="h-4 w-4 mr-2" />
                              Start Streaming
                            </>
                          )}
                        </Button>
                      </div>
                    </div>
                    
                    {uploading && streamingData && (
                      <div className="space-y-2">
                        <div className="flex justify-between text-sm">
                          <span>Streaming progress</span>
                          <span>{uploadProgress}%</span>
                        </div>
                        <div className="w-full bg-secondary rounded-full h-2">
                          <div
                            className="bg-primary h-2 rounded-full transition-all duration-300"
                            style={{ width: `${uploadProgress}%` }}
                          ></div>
                        </div>
                      </div>
                    )}
                    
                    <div className="text-sm text-muted-foreground">
                      <p>• The API should return JSON data in array format</p>
                      <p>• Data will be streamed and validated in real-time</p>
                      <p>• Processing will continue until the stream ends</p>
                    </div>
                  </div>
                </div>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>

        <div className="grid md:grid-cols-3 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <FileText className="h-5 w-5" />
                <span>File Requirements</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2 text-sm">
                <li>• Maximum file size: 100MB</li>
                <li>• Supported formats: CSV, JSON, Excel</li>
                <li>• UTF-8 encoding recommended</li>
                <li>• Headers should be in the first row</li>
              </ul>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Globe className="h-5 w-5" />
                <span>API Streaming</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2 text-sm">
                <li>• JSON array format preferred</li>
                <li>• Real-time data processing</li>
                <li>• Automatic format detection</li>
                <li>• Configurable timeout settings</li>
              </ul>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Database className="h-5 w-5" />
                <span>Validation Process</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2 text-sm">
                <li>• Data quality assessment</li>
                <li>• Schema validation</li>
                <li>• Duplicate detection</li>
                <li>• Missing value analysis</li>
              </ul>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};

export default Upload;