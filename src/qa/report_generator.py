import pandas as pd
import great_expectations as gx
import os

def generate_quality_report(df):
    """
    Force-generates the physical HTML report on your disk.
    """
    # 1. Initialize a Persistent Context 
    # This anchors the folder structure to your desktop
    context = gx.get_context(project_root_dir=os.getcwd())
    
    # 2. Setup Data Source & Asset
    datasource_name = "real_estate_pipeline_ds"
    try:
        datasource = context.data_sources.add_pandas(name=datasource_name)
    except:
        datasource = context.data_sources.get(datasource_name)

    asset_name = "unified_listings_asset"
    try:
        asset = datasource.add_dataframe_asset(name=asset_name)
    except:
        asset = datasource.get_asset(asset_name)

    # 3. Setup Suite & Validator
    suite_name = "quality_rules_suite"
    try:
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    except:
        suite = context.suites.get(suite_name)

    # Use 'options' dictionary to pass the dataframe properly in GX 1.x
    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite_name=suite_name
    )

    # 4. Define Dimensions (Catching the 13 NaNs and 2 zeros found)
    validator.expect_column_values_to_not_be_null("price") # Completeness
    validator.expect_column_values_to_be_between("price", min_value=10000) # Accuracy
    validator.expect_column_values_to_match_regex("listed_date", r"^\d{4}-\d{2}-\d{2}$") # Validity
    validator.expect_column_values_to_be_unique("address") # Uniqueness

    # 5. Run Validation
    validation_result = validator.validate()
    
    # 6. Build the Visual Report (Data Docs)
    print("\n[*] Exporting HTML Quality Report...")
    context.build_data_docs()
    
    report_path = os.path.abspath("gx/uncommitted/data_docs/local_site/index.html")
    print(f"✅ Report created at: {report_path}")
    
    return validation_result