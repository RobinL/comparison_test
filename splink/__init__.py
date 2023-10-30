from dataclasses import dataclass

duckdb_mapping = {"levenshtein": "levenshtein", "jaro_winkler": "jaro_winkler"}
all_dialects_mapping = {"duckdb": duckdb_mapping}


class ComparisonLevel:
    def __init__(self, level_dict) -> None:
        self.level_dict = level_dict


class LazyLevel:
    def __init__(self, dialect=None):
        self.dialect = dialect

    @property
    def dialect_mapping(self):
        if self.dialect is None:
            raise ValueError("Dialect is None so cannot be retrieved")
        return all_dialects_mapping[self.dialect]

    def get_dialected_level(self):
        if self.dialect:
            return ComparisonLevel(self.comparison_level_dict)
        else:
            raise ValueError("No dialect set")


@dataclass
class levenshtein_level(LazyLevel):
    col_name: str
    distance_threshold: int
    dialect: str = None

    @property
    def comparison_level_dict(self):
        lev_fn_name = self.dialect_mapping["levenshtein"]
        sql_cond = (
            f"{lev_fn_name}({self.col_name}_l, {self.col_name}_r) "
            f"<= {self.distance_threshold}"
        )
        level_dict = {
            "sql_condition": sql_cond,
            "label_for_charts": f"{lev_fn_name} <= {self.distance_threshold}",
        }
        return level_dict


# The problem is you still need something like


def levenshtein_level_factory(
    col_name: str, distance_threshold: int, dialect: str = None
):
    lev = levenshtein_level(col_name, distance_threshold, dialect)
    if dialect:
        return ComparisonLevel(lev.comparison_level_dict)
    else:
        return lev
