from splink import DuckDBLinker, levenshtein_level

# lazy_level is of type LazyComparisonLevelFactory
lazy_level = levenshtein_level("name", 2)

settings = {"comparison_level": levenshtein_level("name", 2)}


linker = DuckDBLinker(settings)
linker.comparison_level.level_dict

# Note greedy level
greedy_level = levenshtein_level("name", 2, dialect="duckdb")
greedy_level.level_dict
