import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, useLocation, useNavigate } from "react-router-dom";
import Index from "./pages/Index";
import Upload from "./pages/Upload";
import Results from "./pages/Results";
import Quarantine from "./pages/Quarantine";
import NotFound from "./pages/NotFound";
import Navigation from "./components/Navigation";
import Login from "./pages/Login";
import { useEffect, useState } from "react";

const queryClient = new QueryClient();

function AppRoutes() {
  const [isLoggedIn, setIsLoggedIn] = useState(() => localStorage.getItem("isLoggedIn") === "true");
  const location = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    if (!isLoggedIn && location.pathname !== "/login") {
      navigate("/login", { replace: true });
    }
    if (isLoggedIn && location.pathname === "/login") {
      navigate("/", { replace: true });
    }
  }, [isLoggedIn, location.pathname, navigate]);

  const handleLogin = () => {
    localStorage.setItem("isLoggedIn", "true");
    setIsLoggedIn(true);
    navigate("/");
  };
  const handleLogout = () => {
    localStorage.removeItem("isLoggedIn");
    setIsLoggedIn(false);
    navigate("/login");
  };

  return (
    <div className="min-h-screen bg-background">
      {location.pathname !== "/login" && (
        <Navigation isLoggedIn={isLoggedIn} onLogout={handleLogout} />
      )}
      <Routes>
        <Route path="/login" element={<Login onLogin={handleLogin} />} />
        <Route path="/" element={isLoggedIn ? <Index /> : <Login onLogin={handleLogin} />} />
        <Route path="/upload" element={isLoggedIn ? <Upload /> : <Login onLogin={handleLogin} />} />
        <Route path="/results" element={isLoggedIn ? <Results /> : <Login onLogin={handleLogin} />} />
        <Route path="/quarantine" element={isLoggedIn ? <Quarantine /> : <Login onLogin={handleLogin} />} />
        <Route path="*" element={<NotFound />} />
      </Routes>
    </div>
  );
}

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter>
        <AppRoutes />
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
