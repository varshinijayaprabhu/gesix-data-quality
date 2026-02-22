import { Link } from 'react-router-dom';
import './Landing.css';

export default function Landing() {
  const dimensions = ['Completeness', 'Accuracy', 'Validity', 'Consistency', 'Uniqueness', 'Integrity', 'Lineage'];

  const dimensionDetails = [
    { name: 'Completeness', desc: 'No missing fields; coverage across required attributes.', icon: '✓' },
    { name: 'Accuracy', desc: 'Values match real-world facts and reference data.', icon: '◎' },
    { name: 'Validity', desc: 'Format and rules compliance (dates, numbers, enums).', icon: '◇' },
    { name: 'Consistency', desc: 'Aligned across sources and time with no contradictions.', icon: '≡' },
    { name: 'Uniqueness', desc: 'No unintended duplicates; clear entity resolution.', icon: '1' },
    { name: 'Integrity', desc: 'Referential and structural relationships hold.', icon: '⟷' },
    { name: 'Lineage', desc: 'Full traceability from source to output.', icon: '→' },
  ];

  return (
    <div className="landing">
      <a href="#how-it-works" className="landing-skip">Skip to content</a>
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
          Ingest, unify, and remediate any dataset with a rigorous
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
        <a href="#how-it-works" className="landing-scroll-hint">Explore</a>
      </main>

      <section id="how-it-works" className="landing-features">
        <h2 className="landing-features-title">How it works</h2>
        <span className="landing-title-underline" aria-hidden="true" />
        <div className="features-grid">
          <div className="feature-card feature-card-1">
            <span className="feature-icon" aria-hidden="true">1</span>
            <h3>Universal Ingestion</h3>
            <p>API, Web portal, and custom CSV uploads with raw archival for full lineage.</p>
          </div>
          <div className="feature-card feature-card-2">
            <span className="feature-icon" aria-hidden="true">2</span>
            <h3>Unify & Clean</h3>
            <p>Dynamic schema alignment, statistical normalization, and smart handling of outliers.</p>
          </div>
          <div className="feature-card feature-card-3">
            <span className="feature-icon" aria-hidden="true">3</span>
            <h3>7-D QA Engine</h3>
            <p>Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, and Lineage—scored in one pass.</p>
          </div>
          <div className="feature-card feature-card-4">
            <span className="feature-icon" aria-hidden="true">4</span>
            <h3>Dashboard & Export</h3>
            <p>Run new analysis on any dataset, view trustability scores, and download reports.</p>
          </div>
        </div>
      </section>

      <section className="landing-dimensions" aria-labelledby="dimensions-heading">
        <h2 id="dimensions-heading" className="landing-section-title">The seven dimensions</h2>
        <span className="landing-title-underline" aria-hidden="true" />
        <p className="landing-section-subtitle">
          Each dimension is scored in a single pass so you get a complete trustability picture.
        </p>
        <div className="dimensions-grid">
          {dimensionDetails.map((d, i) => (
            <div key={d.name} className="dimension-card" style={{ animationDelay: `${i * 0.06}s` }}>
              <span className="dimension-icon" aria-hidden="true">{d.icon}</span>
              <h3>{d.name}</h3>
              <p>{d.desc}</p>
            </div>
          ))}
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

      <section className="landing-cta-section">
        <p className="landing-cta-section-text">Ready to improve your data trustability?</p>
        <Link to="/dashboard" className="landing-cta landing-cta-secondary">Open Dashboard</Link>
      </section>

      <footer className="landing-footer">
        <div className="landing-footer-inner">
          <Link to="/" className="landing-footer-brand">
            <span className="landing-brand-mark">G</span>
            <span>Gesix</span>
          </Link>
          <nav className="landing-footer-nav">
            <Link to="/dashboard">Dashboard</Link>
            <span className="landing-footer-sep">·</span>
            <span className="landing-footer-muted">Data Quality Framework</span>
          </nav>
        </div>
        <p className="landing-footer-copy">© {new Date().getFullYear()} Gesix. One framework, seven dimensions.</p>
      </footer>
    </div>
  );
}
