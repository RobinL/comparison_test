from splink import DuckDBLinker, LevenshteinLevel, ComparisonLevelCreator

#########################
# Suggested Usage Pattern (the settings object here does not have its full complexity)
#########################
ll = LevenshteinLevel("name", 2)
ll.m_probability = 0.9

settings = {"comparison_level": LevenshteinLevel("name", 2)}
linker = DuckDBLinker(settings)
linker.comparison_level.level_dict


# Create comparison level
cl = LevenshteinLevel("name", 2, "duckdb").get_comparison_level()


LevenshteinLevel("name", distance_threshold=2).configure(
    m_probability=0.9, u_probability=0.2
).get_comparison_level("duckdb")


LevenshteinLevel("name", distance_threshold=2).configure(
    m_probability=0.9, u_probability=0.2
).get_comparison_level("quackdb")
