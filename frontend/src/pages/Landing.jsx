import { Link } from 'react-router-dom';
import './Landing.css';

export default function Landing() {
  const dimensions = ['Completeness', 'Accuracy', 'Validity', 'Consistency', 'Uniqueness', 'Integrity', 'Lineage'];

  return (
    <div className="landing">
      <div className="landing-bg" aria-hidden="true">
        <div className="landing-bg-grid" />
        <div className="landing-bg-glow landing-bg-glow-1" />
        <div className="landing-bg-glow landing-bg-glow-2" />
      </div>

      <header className="landing-header">
        <div className="landing-brand">
          <span className="landing-brand-mark">G</span>
          <span>Gesix</span>
        </div>
        <nav className="landing-nav">
          <Link to="/dashboard">Dashboard</Link>
        </nav>
      </header>

      <main className="landing-hero">
        <p className="landing-eyebrow">Data Quality & Trustability</p>
        <h1 className="landing-title">
          One framework.
          <br />
          <span className="landing-title-accent">Seven dimensions.</span>
        </h1>
        <p className="landing-subtitle">
          Ingest, unify, and remediate real estate data with a rigorous
          quality engine—built for reliable AI and analytics.
        </p>
        <div className="landing-cta-wrap">
          <Link to="/dashboard" className="landing-cta">
            Open Dashboard
          </Link>
          <span className="landing-cta-hint">Run analysis · View scores · Export PDF</span>
        </div>
        <div className="landing-dims" aria-hidden="true">
          {dimensions.map((d, i) => (
            <span key={d} className="landing-dim-pill" style={{ animationDelay: `${i * 0.05}s` }}>
              {d}
            </span>
          ))}
        </div>
      </main>

      <section className="landing-features">
        <h2 className="landing-features-title">How it works</h2>
        <div className="features-grid">
          <div className="feature-card feature-card-1">
            <span className="feature-icon" aria-hidden="true">1</span>
            <h3>Ingestion</h3>
            <p>API and city records with date-range filtering and raw archival for full lineage.</p>
          </div>
          <div className="feature-card feature-card-2">
            <span className="feature-icon" aria-hidden="true">2</span>
            <h3>Unify & Clean</h3>
            <p>Single schema, address normalization, and smart handling of missing or invalid prices.</p>
          </div>
          <div className="feature-card feature-card-3">
            <span className="feature-icon" aria-hidden="true">3</span>
            <h3>7-D QA Engine</h3>
            <p>Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, and Lineage—scored in one pass.</p>
          </div>
          <div className="feature-card feature-card-4">
            <span className="feature-icon" aria-hidden="true">4</span>
            <h3>Dashboard & Export</h3>
            <p>Run new analysis, view trustability scores, and download reports as PDF.</p>
          </div>
        </div>
      </section>

      <section className="landing-stats">
        <div className="landing-stat">
          <span className="landing-stat-value">7</span>
          <span className="landing-stat-label">Quality dimensions</span>
        </div>
        <div className="landing-stat">
          <span className="landing-stat-value">1</span>
          <span className="landing-stat-label">Unified pipeline</span>
        </div>
        <div className="landing-stat">
          <span className="landing-stat-value">100%</span>
          <span className="landing-stat-label">Traceable lineage</span>
        </div>
      </section>

      <footer className="landing-footer">
        <p>Gesix Data Quality Framework</p>
      </footer>
    </div>
  );
}
