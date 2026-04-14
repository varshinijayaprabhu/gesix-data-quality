import { Link, useLocation } from "react-router-dom";
import { ThemeToggle } from "./theme-toggle";

export default function Navbar() {
  const { pathname } = useLocation();
  return (
    <header className="sticky top-0 z-50 w-full border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container mx-auto px-4 lg:px-8 h-16 flex items-center justify-between">
        {/* Left Section */}
        <div className="flex items-center gap-6">
          <Link
            to="/"
            className="flex items-center gap-2 font-serif text-lg md:text-xl font-medium tracking-tight"
          >
            <span>Data Quality and Trustability Framework</span>
          </Link>
        </div>

        {/* Center/Right Section */}
        <div className="flex items-center gap-3 md:gap-6">
          <nav className="hidden md:flex items-center gap-6 text-sm font-medium">
            <Link
              to="/"
              className={`transition-colors ${pathname === "/" ? "text-foreground font-semibold" : "text-muted-foreground hover:text-foreground"}`}
            >
              Home
            </Link>
            <Link
              to="/dashboard"
              className={`transition-colors ${pathname === "/dashboard" ? "text-foreground font-semibold" : "text-muted-foreground hover:text-foreground"}`}
            >
              Dashboard
            </Link>
          </nav>

          <div className="h-5 w-px bg-border hidden md:block" />

          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
