import { Link } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { Card, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Activity, ShieldCheck, Zap, Database, ArrowRight, FileText } from 'lucide-react';

const FEATURES = [
  { id: '1', title: 'Universal Ingestion',  desc: 'API, Web portal, and custom CSV uploads with raw archival for full lineage.', icon: Database },
  { id: '2', title: 'Unify & Clean',        desc: 'Dynamic schema alignment, statistical normalization, and smart handling of outliers.', icon: Zap },
  { id: '3', title: '7-D QA Engine',        desc: 'Completeness, Accuracy, Validity, Consistency, Uniqueness, Integrity, and Lineage scored in one pass.', icon: ShieldCheck },
  { id: '4', title: 'Dashboard & Export',   desc: 'Run new analysis on any dataset, view trustability scores, and download reports.', icon: Activity },
];

const DIMENSIONS = [
  { name: 'Completeness', icon: '✓', desc: 'No missing fields; coverage across required attributes.' },
  { name: 'Accuracy',     icon: '◎', desc: 'Values match real-world facts and reference data.' },
  { name: 'Validity',     icon: '◇', desc: 'Format and rules compliance (dates, numbers, enums).' },
  { name: 'Consistency',  icon: '≡', desc: 'Aligned across sources and time with no contradictions.' },
  { name: 'Uniqueness',   icon: '1', desc: 'No unintended duplicates; clear entity resolution.' },
  { name: 'Integrity',    icon: '⟷', desc: 'Referential and structural relationships hold.' },
  { name: 'Lineage',      icon: '→', desc: 'Full traceability from source to output.' },
];


function BgBlobs() {
  return (
    <div className="absolute inset-0 overflow-hidden pointer-events-none -z-10 bg-background">
      {/* Primary Glow */}
      <div className="absolute top-[-10%] left-[-10%] w-[50%] h-[50%] rounded-full bg-primary/20 blur-[120px] mix-blend-screen dark:mix-blend-lighten animate-in fade-in duration-1000" />
      {/* Secondary Glow */}
      <div className="absolute top-[20%] right-[-10%] w-[40%] h-[40%] rounded-full bg-secondary/30 blur-[150px] mix-blend-screen dark:mix-blend-lighten animate-in fade-in duration-1000 delay-300" />
      {/* Gold Accent */}
      <div className="absolute bottom-[-10%] left-[20%] w-[30%] h-[30%] rounded-full bg-gold/15 blur-[100px] mix-blend-screen dark:mix-blend-lighten animate-in fade-in duration-1000 delay-500" />
    </div>
  );
}

function SectionHeading({ title, subtitle, barClass = 'w-12 h-1' }) {
  return (
    <>
      <h2 className="text-4xl md:text-5xl font-serif font-bold text-foreground mb-6 tracking-tight">{title}</h2>
      <div className={`${barClass} bg-gradient-to-r from-primary to-transparent mb-8 rounded-full`} />
      {subtitle && (
        <p className="text-lg md:text-xl text-muted-foreground leading-relaxed max-w-2xl">{subtitle}</p>
      )}
    </>
  );
}


export default function Home() {
  return (
    <div className="flex flex-col min-h-screen relative font-sans overflow-hidden">
      <BgBlobs />

      <main className="flex-1 flex flex-col relative z-10">

        {/* Hero */}
        <section className="relative px-4 pt-32 pb-32 md:pt-48 md:pb-48 container mx-auto text-center flex flex-col items-center justify-center min-h-[90vh]">
          
          <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full border border-primary/20 bg-primary/5 text-primary text-xs font-bold tracking-widest uppercase mb-8 animate-in slide-in-from-bottom-4 fade-in duration-700 backdrop-blur-sm shadow-sm hover:bg-primary/10 transition-colors">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-primary opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-primary"></span>
            </span>
            Data Quality & Trustability
          </div>
          
          <h1 className="text-5xl md:text-7xl lg:text-8xl font-serif text-foreground font-black tracking-tight leading-[1.1] mb-8 animate-in slide-in-from-bottom-6 fade-in duration-700 delay-100 max-w-5xl mx-auto drop-shadow-sm">
            One Framework.<br />
            <span className="gradient-text italic pr-2">Seven Dimensions.</span>
          </h1>
          
          <p className="max-w-2xl mx-auto text-lg md:text-2xl text-muted-foreground mb-12 leading-relaxed animate-in slide-in-from-bottom-6 fade-in duration-700 delay-200 font-medium">
            Ingest, unify, and remediate any dataset with a rigorous quality engine built for reliable AI and analytics.
          </p>
          
          <div className="flex flex-col sm:flex-row items-center justify-center gap-6 mb-8 animate-in slide-in-from-bottom-6 fade-in duration-700 delay-300 w-full sm:w-auto">
            <Button size="lg" asChild className="btn-primary h-14 px-8 text-lg w-full sm:w-auto">
              <Link to="/dashboard" className="flex items-center gap-2">
                Open Dashboard <ArrowRight className="w-5 h-5 ml-1" />
              </Link>
            </Button>
            <Button size="lg" variant="outline" asChild className="h-14 px-8 text-lg w-full sm:w-auto border-2 border-border/80 hover:bg-secondary/20 hover:border-primary/50 text-foreground transition-all duration-300 rounded-xl shadow-sm backdrop-blur-sm bg-background/50">
                <a href="#how-it-works">How it works</a>
            </Button>
          </div>
          
          <div className="flex items-center justify-center gap-4 text-xs text-muted-foreground font-bold tracking-[0.15em] uppercase animate-in fade-in duration-700 delay-500">
            <span className="flex items-center gap-1.5"><Activity className="w-3.5 h-3.5 text-primary" /> Run Analysis</span>
            <span className="w-1 h-1 rounded-full bg-border"></span>
            <span className="flex items-center gap-1.5"><ShieldCheck className="w-3.5 h-3.5 text-primary" /> View Scores</span>
            <span className="w-1 h-1 rounded-full bg-border"></span>
            <span className="flex items-center gap-1.5"><FileText className="w-3.5 h-3.5 text-primary" /> Export PDF</span>
          </div>

        </section>

        {/* How it works */}
        <section id="how-it-works" className="py-24 md:py-32 relative">
          <div className="absolute inset-0 bg-secondary/5 skew-y-[-2deg] origin-top-left -z-10" />
          <div className="container mx-auto px-4 lg:px-8">
            <div className="max-w-5xl mb-20 md:mb-28 text-center mx-auto flex flex-col items-center">
              <SectionHeading
                title="How it works"
                subtitle="A unified pipeline to bring trustability to your data workflows."
                barClass="w-24 h-1.5"
              />
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8 lg:gap-12 max-w-6xl mx-auto">
              {FEATURES.map((f, i) => {
                  const Icon = f.icon;
                  return (
                <div key={f.id} className="surface-glass rounded-3xl p-8 md:p-10 transition-all duration-500 hover:shadow-2xl hover:shadow-primary/10 hover:-translate-y-2 group relative overflow-hidden">
                  <div className="absolute top-0 right-0 p-8 text-8xl font-serif italic text-primary/5 dark:text-primary/10 select-none pointer-events-none transition-transform duration-500 group-hover:scale-110 group-hover:-rotate-6">
                    0{f.id}
                  </div>
                  <div className="relative z-10 flex flex-col h-full">
                    <div className="w-14 h-14 rounded-2xl bg-primary/10 text-primary flex items-center justify-center mb-6 group-hover:scale-110 transition-transform duration-500 shadow-inner">
                      <Icon className="w-7 h-7" />
                    </div>
                    <h3 className="text-2xl font-serif font-bold mb-4 text-foreground tracking-tight">{f.title}</h3>
                    <p className="text-muted-foreground text-lg leading-relaxed flex-grow">{f.desc}</p>
                  </div>
                </div>
              )})}
            </div>
          </div>
        </section>

        {/* Seven Dimensions */}
        <section className="py-24 md:py-40 container mx-auto px-4 lg:px-8">
          <div className="max-w-3xl mb-16 md:mb-24">
            <SectionHeading
              title="The Seven Dimensions"
              subtitle="Each dimension is scored in a single pass so you get a complete trustability picture."
              barClass="w-32 h-1.5 rounded-full"
            />
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
            {DIMENSIONS.map((d, index) => (
              <Card key={d.name} className="flex flex-col h-full surface-glass border-border/60 hover:border-primary/40 transition-all duration-500 hover:shadow-xl hover:shadow-primary/5 hover:-translate-y-2 group">
                <CardHeader className="flex-1 p-6 md:p-8">
                  <div className="text-5xl mb-6 text-muted-foreground/40 group-hover:text-primary transition-all duration-500 font-serif italic drop-shadow-sm group-hover:scale-110 origin-left">
                    {d.icon}
                  </div>
                  <CardTitle className="text-2xl font-bold tracking-tight mb-3 group-hover:text-primary transition-colors">{d.name}</CardTitle>
                  <CardDescription className="text-base leading-relaxed text-muted-foreground font-medium">{d.desc}</CardDescription>
                </CardHeader>
              </Card>
            ))}
          </div>
        </section>

      </main>
    </div>
  );
}
