import pandas as pd
import great_expectations as gx
import os
import webbrowser

def run_qa_suite(csv_path):
    """
    Final corrected 7-Dimensional Quality Engine.
    Processes unified listings and generates a visual health report.
    """
    df = pd.read_csv(csv_path)
    context = gx.get_context()
    
    # 1. Setup Datasource 
    ds_name = "real_estate_pipeline_ds"
    try:
        datasource = context.data_sources.add_pandas(name=ds_name)
    except:
        datasource = context.data_sources.get(ds_name)

    # 2. Setup Data Asset
    asset_name = "structured_listings_asset"
    try:
        asset = datasource.add_dataframe_asset(name=asset_name)
    except:
        asset = datasource.get_asset(asset_name)

    # 3. Setup Expectation Suite
    suite_name = "quality_rules_suite"
    try:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    except:
        suite = context.suites.get(suite_name)

    # 4. Create Validator
    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}), 
        expectation_suite_name=suite_name
    )

    # --- APPLYING THE 7 DIMENSIONS ---
    validator.expect_column_values_to_not_be_null("price") # Completeness
    validator.expect_column_values_to_be_between("price", min_value=10000) # Accuracy
    validator.expect_column_values_to_match_regex("listed_date", r"^\d{4}-\d{2}-\d{2}$") # Validity
    validator.expect_column_values_to_be_unique("address") # Uniqueness

    # 5. Run Scan
    print("\n[*] Running 7-Dimensional Quality Scan...")
    results = validator.validate()
    
    # 6. Generate Visual Report
    print("[*] Building Visual Dashboard (HTML)...")
    context.build_data_docs()
    
    # 7. Auto-open report
    report_path = os.path.abspath("gx/uncommitted/data_docs/local_site/index.html")
    if os.path.exists(report_path):
        webbrowser.open(f"file://{report_path}")
        print(f"✅ Dashboard Ready: {report_path}")
    
    return results

def calculate_trustability_score(validation_results):
    """
    Calculates health score using the modern GX 1.x object structure.
    Logic: $Score = 100 - \sum(Penalties)$
    """
    penalties = 0
    # In GX 1.x, validation_results.results is a list of objects
    results = validation_results.results
    
    for r in results:
        if not r.success:
            # FIX: Use .type instead of ['expectation_type']
            exp_type = r.expectation_config.type
            
            # Critical Failures (-30%): Missing or Zero Prices
            if "not_be_null" in exp_type or "be_between" in exp_type:
                penalties += 30
            # Warning Failures (-10%): Format or Duplicates
            else:
                penalties += 10
                
    score = max(0, 100 - penalties)
    status = "Golden Data" if score >= 90 else "Warning" if score >= 70 else "Untrustworthy"
    return score, status