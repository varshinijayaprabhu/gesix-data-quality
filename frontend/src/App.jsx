import React from 'react';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { ThemeProvider } from './components/theme-provider';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import Navbar from './components/Navbar';

const App = () => {
  return (
    <ThemeProvider defaultTheme="light" storageKey="dqt-ui-theme">
      <BrowserRouter>
        <div className="min-h-screen bg-background text-foreground flex flex-col font-sans selection:bg-primary/20">
          <Navbar />
          <main className="flex-1 flex flex-col">
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/dashboard" element={<Dashboard />} />
            </Routes>
          </main>
        </div>
      </BrowserRouter>
    </ThemeProvider>
  );
};

export default App;