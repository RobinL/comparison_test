# TODO:
# Better error message if fn not available for backend - wrap dicts in class or something?

import warnings
from sqlglot.expressions import Identifier

duckdb_mapping = {"levenshtein": "levenshtein", "jaro_winkler": "jaro_winkler"}
athena_mapping = {"levenshtein": "levenshtein_distance"}
all_dialects_mapping = {"duckdb": duckdb_mapping, "presto": athena_mapping}


def lookup_dialect(dialect, key):
    try:
        dialect_mapping = all_dialects_mapping[dialect]
    except KeyError:
        available_dialects = ", ".join(all_dialects_mapping.keys())
        raise KeyError(
            f"Dialect '{dialect}' not found. Available dialects"
            f" are: {available_dialects}"
        )

    try:
        return dialect_mapping[key]
    except KeyError:
        raise NotImplementedError(
            "The comparison level you've asked for is not implemented for the "
            f"{dialect} dialect"
        )


class Linker:
    def __init__(self, settings_dict: dict):
        if self.dialect:
            cl = settings_dict["comparison_level"]
            self.comparison_level = cl.get_dialected_level(dialect=self.dialect)


class DuckDBLinker(Linker):
    def __init__(self, settings_dict: dict):
        self.dialect = "duckdb"
        super().__init__(settings_dict)


class AthenaLinker(Linker):
    def __init__(self, settings_dict: dict):
        self.dialect = "presto"
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


class InputColumn:
    def __init__(self, raw_name, sql_dialect=None):
        self.sql_dialect = sql_dialect
        self.raw_name = raw_name
        self.name = Identifier(this=raw_name, quoted=True)

    @property
    def name_sql(self):
        return self.name.sql(dialect=self.sql_dialect)

    @property
    def name_l(self):
        return Identifier(this=self.raw_name + "_l", quoted=True).sql(
            dialect=self.sql_dialect
        )

    @property
    def name_r(self):
        return Identifier(this=self.raw_name + "_r", quoted=True).sql(
            dialect=self.sql_dialect
        )


def distance_function_level(
    col_name: str,
    distance_function_name: str,
    distance_threshold: int | float,
    higher_is_more_similar: bool = True,
    dialect=None,
) -> ComparisonLevel:
    if not dialect:
        kwargs = locals()
        return LazyComparisonLevelFactory(distance_function_level, **kwargs)

    if higher_is_more_similar:
        operator = ">="
    else:
        operator = "<="

    input_column = InputColumn(col_name, dialect)

    sql_cond = (
        f"{distance_function_name}({input_column.name_l}, {input_column.name_r}) "
        f"{operator} {distance_threshold}"
    )
    level_dict = {
        "sql_condition": sql_cond,
        "label_for_charts": f"{distance_function_name} {operator} {distance_threshold}",
    }

    return ComparisonLevel(level_dict)


def levenshtein_level(
    col_name: str, distance_threshold: int, dialect=None
) -> ComparisonLevel:
    """Amazing docstring

    Args:
        col_name (str): blah
        distance_threshold (int): blah
        dialect (_type_, optional): blah. Defaults to None.

    Returns:
        ComparisonLevel: blah
    """
    if not dialect:
        kwargs = locals()
        return LazyComparisonLevelFactory(levenshtein_level, **kwargs)

    lev_fn_name = lookup_dialect(dialect, "levenshtein")

    return distance_function_level(
        col_name, lev_fn_name, distance_threshold, False, dialect
    )


def jaro_winkler_level(
    col_name: str, distance_threshold: int, dialect=None
) -> ComparisonLevel:
    if not dialect:
        kwargs = locals()
        return LazyComparisonLevelFactory(jaro_winkler_level, **kwargs)

    jw_fn_name = lookup_dialect(dialect, "jaro_winkler")

    return distance_function_level(
        col_name, jw_fn_name, distance_threshold, False, dialect
    )
