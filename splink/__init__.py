import warnings

duckdb_mapping = {
    "levenshtein": "levenshtein",
    "jaro_winkler": "jaro_winkler",
}
all_dialects_mapping = {"duckdb": duckdb_mapping}


class Linker:
    def __init__(self, settings_dict: dict):
        if self.dialect:
            cl = settings_dict["comparison_level"]
            self.comparison_level = cl.activate(dialect=self.dialect)


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
        del self.kwargs["dialect"]

    def activate(self, dialect):
        return self.comparison_level_function(dialect=dialect, **self.kwargs)

    def __getattr__(self, name):
        if name != "activate":
            warnings.warn(
                "This comparison level cannot be used directly because it doesn't have "
                "a dialect associated with it.\nEither pass a dialect to it when you "
                "create it, or obtain an activated version by calling\n"
                "activated_level = this_level.activate(dialect)\n"
                'eg. this_level.activate("duckdb"))'
            )


def levenshtein_level(
    col_name: str, distance_threshold: int, dialect=None
) -> ComparisonLevel:
    if not dialect:
        kwargs = locals()
        return LazyComparisonLevelFactory(levenshtein_level, **kwargs)

    dialect_mapping = all_dialects_mapping[dialect]

    lev_fn_name = dialect_mapping["levenshtein"]
    sql_cond = f"{lev_fn_name}({col_name}_l, {col_name}_r) " f"<= {distance_threshold}"
    level_dict = {
        "sql_condition": sql_cond,
        "label_for_charts": f"{lev_fn_name} <= {distance_threshold}",
    }
    return ComparisonLevel(level_dict)
