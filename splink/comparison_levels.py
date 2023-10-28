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
            self.comparison_level = cl.activate()


class DuckDBLinker:
    def __init__(self, settings_dict: dict):
        self.dialect = "duckdb"
        super()


class ComparisonLevel:
    def __init__(self, level_dict) -> None:
        self.level_dict


class LazyComparisonLevelFactory:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def activate(self, greedy_comparison_level_fn):
        return greedy_comparison_level_fn(*self.args, **self.kwargs)


def levenshtein_level(
    col_name: str,
    distance_threshold: int,
) -> ComparisonLevel:
    sql_cond = f"{lev_fn_name}({col_name}_l, {col_name}_r) " f"<= {distance_threshold}"
    level_dict = {
        "sql_condition": sql_cond,
        "label_for_charts": f"{lev_fn_name} <= {distance_threshold}",
    }

    return LazyComparisonLevelFactory(level_dict)
