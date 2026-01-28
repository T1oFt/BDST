from airflow.providers.postgres.hooks.postgres import PostgresHook


def check_ods_dds_consistency(pg_hook: PostgresHook):
    dds_queries = {
        "parties": "SELECT name FROM dds.parties WHERE name IS NOT NULL",
        "datasets": "SELECT name FROM dds.dim_datasets WHERE name IS NOT NULL", 
        "authors": "SELECT name FROM dds.dim_authors WHERE name IS NOT NULL",
        "keywords": "SELECT name FROM dds.dim_keywords WHERE name IS NOT NULL"
    }
    
    dds_uniques = {}
    for key, sql in dds_queries.items():
        dds_uniques[key] = {row[0] for row in pg_hook.get_records(sql)}
    
    results = {}
    
    parties_sql = """
        SELECT owner AS party FROM ods.models WHERE owner IS NOT NULL
        UNION ALL
        SELECT submitted_by FROM ods.papers WHERE submitted_by IS NOT NULL  
        UNION ALL
        SELECT organization FROM ods.papers WHERE organization IS NOT NULL
        UNION ALL
        SELECT author FROM ods.posts WHERE author IS NOT NULL
        UNION ALL
        SELECT users_value::text AS party
        FROM ods.posts p,
            jsonb_array_elements(p.reactions) AS reaction_elem,
            jsonb_array_elements_text(reaction_elem->'users') AS users_value
        WHERE p.reactions IS NOT NULL 
            AND jsonb_typeof(p.reactions) = 'array'
            AND reaction_elem ? 'users'
            AND jsonb_typeof(reaction_elem->'users') = 'array'
    """
    parties_all = pg_hook.get_records(parties_sql)
    parties_total_count = len(parties_all)
    parties_unique_ods = {row[0] for row in parties_all}
    
    results["parties"] = {
        "ods_total_count": parties_total_count,
        "ods_unique_count": len(parties_unique_ods),
        "dds_unique_count": len(dds_uniques["parties"]),
        "missing_in_dds": sorted(parties_unique_ods - dds_uniques["parties"])[:10]
    }
    
    datasets_sql = """
        SELECT unnest(dataset) AS ds 
        FROM ods.models 
        WHERE dataset IS NOT NULL AND array_length(dataset, 1) > 0
    """
    datasets_all = pg_hook.get_records(datasets_sql)
    datasets_total_count = len(datasets_all)
    datasets_unique_ods = {row[0] for row in datasets_all}
    
    results["datasets"] = {
        "ods_total_count": datasets_total_count,
        "ods_unique_count": len(datasets_unique_ods),
        "dds_unique_count": len(dds_uniques["datasets"]),
        "missing_in_dds": sorted(datasets_unique_ods - dds_uniques["datasets"])[:10]
    }
    
    authors_sql = """
        SELECT unnest(authors) AS a
        FROM ods.papers 
        WHERE authors IS NOT NULL AND array_length(authors, 1) > 0
    """
    authors_all = pg_hook.get_records(authors_sql)
    authors_total_count = len(authors_all)
    authors_unique_ods = {row[0] for row in authors_all}
    
    results["authors"] = {
        "ods_total_count": authors_total_count,
        "ods_unique_count": len(authors_unique_ods),
        "dds_unique_count": len(dds_uniques["authors"]),
        "missing_in_dds": sorted(authors_unique_ods - dds_uniques["authors"])[:10]
    }
    
    keywords_sql = """
        SELECT unnest(ai_keywords) AS k
        FROM ods.papers 
        WHERE ai_keywords IS NOT NULL AND array_length(ai_keywords, 1) > 0
    """
    keywords_all = pg_hook.get_records(keywords_sql)
    keywords_total_count = len(keywords_all)
    keywords_unique_ods = {row[0] for row in keywords_all}
    
    results["keywords"] = {
        "ods_total_count": keywords_total_count,
        "ods_unique_count": len(keywords_unique_ods),
        "dds_unique_count": len(dds_uniques["keywords"]),
        "missing_in_dds": sorted(keywords_unique_ods - dds_uniques["keywords"])[:10]
    }
    
    errors = []
    print("\n=== ODS vs DDS Dictionary Coverage ===")
    print("| Справочник | ODS всего | ODS уник. | DDS уник. | DDS покрытие |")
    print("|------------|-----------|-----------|-----------|--------------|")
    
    for key, data in results.items():
        coverage = "✓" if not data["missing_in_dds"] else "✗"
        print(f"| {key:<9} | {data['ods_total_count']:<9} | "
              f"{data['ods_unique_count']:<9} | {data['dds_unique_count']:<9} | "
              f"{coverage:<12} |")
        
        if data["missing_in_dds"]:
            errors.append(f"{key}: {len(data['missing_in_dds'])} пропало - {data['missing_in_dds']}")
    
    if errors:
        for error in errors:
            print(f"  - {error}")
        raise ValueError(f"Dictionary mismatch: {len(errors)} failures")
    
    print("\n=== Done ===")
    return results
