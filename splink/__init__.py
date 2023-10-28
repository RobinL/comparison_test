duckdb_mapping = {
    "levenshtein": "levenshtein",
    "jaro_winkler": "jaro_winkler",
}
mappings = {"duckdb": duckdb_mapping}


class Linker:
    def __init__(self, settings_dict: dict):
        if self.dialect:
            dialect_mapping = mappings["duckdb"]
            cl = settings_dict["comparison_level"]
            self.comparison_level = cl.activate(dialect_mapping=dialect_mapping)


class DuckDBLinker(Linker):
    def __init__(self, settings_dict: dict):
        self.dialect = "duckdb"
        super().__init__(settings_dict)


class ComparisonLevel:
    def __init__(self, level_dict) -> None:
        self.level_dict = level_dict


class LazyComparisonLevelFactory:
    def __init__(self, comparison_level_function, **kwargs):
        self.comparison_level_function = comparison_level_function
        self.kwargs = kwargs

    def activate(self, dialect_mapping):
        del self.kwargs["dialect_mapping"]
        return self.comparison_level_function(
            dialect_mapping=dialect_mapping, **self.kwargs
        )


def levenshtein_level(
    col_name: str, distance_threshold: int, dialect_mapping=None
) -> ComparisonLevel:
    if not dialect_mapping:
        kwargs = locals()
        return LazyComparisonLevelFactory(levenshtein_level, **kwargs)

    lev_fn_name = dialect_mapping["levenshtein"]
    sql_cond = f"{lev_fn_name}({col_name}_l, {col_name}_r) " f"<= {distance_threshold}"
    level_dict = {
        "sql_condition": sql_cond,
        "label_for_charts": f"{lev_fn_name} <= {distance_threshold}",
    }
    return ComparisonLevel(level_dict)
