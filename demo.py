from splink import DuckDBLinker, levenshtein_level

#########################
# Suggested Usage Pattern (the settings object here does not have its full complexity)
#########################
settings = {"comparison_level": levenshtein_level("name", 2)}
linker = DuckDBLinker(settings)


#########################
# More detailed demo of how the levenshtein_level works:
#########################

# To get a ComparisonLevel, call the levenshtein_level with a dialect
cl = levenshtein_level("name", 2, dialect="duckdb")

# Get a lazy level, omit the dialect.  This returns a LazyComparisonLevelFactory
lazy_level = levenshtein_level("name", 2)

# If you ask for a ComparisonLevel method on a LazyComparisonLevelFactory
# you get a helpful warning
lazy_level.level_dict

# You can manually activate a LazyComparisonLevelFactory to get a ComparisonLevel
cl = lazy_level.get_activated_level("duckdb")


# Highly simplified usage
settings = {"comparison_level": levenshtein_level("name", 2)}

# On init, activates all LazyComparisonLevelFactories, so they are now ComparisonLevels
linker = DuckDBLinker(settings)

# e.g. this works
linker.comparison_level.level_dict
