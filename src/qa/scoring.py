def calculate_trustability_score(validation_results):
    """
    Calculates health score using the modern GX 1.x object structure.
    Logic: $Score = 100 - \sum(Penalties)$
    """
    penalties = 0
    # validation_results.results is a list of ExpectationValidationResult objects
    results = validation_results.results
    
    for r in results:
        if not r.success:
            # FIX: Use .type instead of ['expectation_type'] or .expectation_type
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