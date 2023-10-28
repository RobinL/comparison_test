from splink import DuckDBLinker, levenshtein_level


settings = {"comparison_level": levenshtein_level("name", 2)}

# levenshtein_level("name", 2) is of type LazyComparisonLevelFactory

linker = DuckDBLinker(settings)
