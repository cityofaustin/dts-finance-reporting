MICROSTRATEGY_DATASET_CONFIG = {
    "sif_spent": {
        # Right-click report -> properties -> ID
        "report_id": "BD8AAB0F184E6D3E91775CA44E472D63",
        "dataset_id": "xac3-86nr",
        "transforms": [
            {"name": "snake_case_keys"},
            {"name": "socrata_datetime", "params": {"column": "date"}},
            {"name": "fiscal_quarter", "params": {"column": "date"}},
        ],
    }
}
