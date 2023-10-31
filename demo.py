from splink import DuckDBLinker, LevenshteinLevel

#########################
# Suggested Usage Pattern (the settings object here does not have its full complexity)
#########################
settings = {"comparison_level": LevenshteinLevel("name", 2)}
linker = DuckDBLinker(settings)
linker.comparison_level.level_dict

# Create comparison level
cl = LevenshteinLevel("name", 2, "duckdb").get_comparison_level()

# Errors
LevenshteinLevel("name", 2).get_comparison_level()
