from splink import levenshtein_level

lev_lazy_level = levenshtein_level("mycol", 2)
lev_lazy_level.comparison_level_dict

lev_lazy_level = levenshtein_level("mycol", 2, "duckdb")
lev_lazy_level.comparison_level_dict
