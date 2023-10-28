import warnings

duckdb_mapping = {"levenshtein": "levenshtein", "jaro_winkler": "jaro_winkler"}
all_dialects_mapping = {"duckdb": duckdb_mapping}


class Linker:
    def __init__(self, settings_dict: dict):
        if self.dialect:
            cl = settings_dict["comparison_level"]
            self.comparison_level = cl.get_dialected_level(dialect=self.dialect)


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

    def get_dialected_level(self, dialect):
        return self.comparison_level_function(dialect=dialect, **self.kwargs)

    def __getattr__(self, name):
        if name != "get_dialected_level":
            warnings.warn(
                "This comparison level cannot be called directly because it doesn't have "
                "a dialect associated with it.\nEither pass a dialect to it when you "
                "create it, or obtain an activated version by calling\n"
                "comparison_level = comparison_level.get_dialected_level(dialect)\n"
                'eg. comparison_level.get_dialected_level("duckdb"))\n'
                "Note this will happen automatically when this is passed into the "
                "linker as part of the settings"
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
