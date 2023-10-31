from dataclasses import dataclass

duckdb_mapping = {"levenshtein": "levenshtein", "jaro_winkler": "jaro_winkler"}
all_dialects_mapping = {"duckdb": duckdb_mapping}


class Linker:
    def __init__(self, settings_dict: dict):
        cl = settings_dict["comparison_level"]
        self.comparison_level = cl.get_comparison_level(self.dialect)


class DuckDBLinker(Linker):
    def __init__(self, settings_dict: dict):
        self.dialect = "duckdb"
        super().__init__(settings_dict)


class ComparisonLevel:
    def __init__(self, level_dict) -> None:
        self.level_dict = level_dict


@dataclass
class InputColumn:
    col_name: str
    dialect: str


@dataclass
class ComparisonLevelCreator:
    def create_sql(self):
        raise NotImplementedError("mag")

    def create_label_for_charts(self):
        raise NotImplementedError("mag")

    def get_comparison_level(self, dialect=None):
        if dialect:
            self.dialect = dialect
        return ComparisonLevel(self.create_level_dict())

    def create_level_dict(self):
        level_dict = {
            "sql_condition": self.create_sql(),
            "label_for_charts": self.create_label_for_charts(),
        }
        return level_dict

    @property
    def input_column(self):
        # To create a comparison level, a dialect is essential even if no special
        # SQL functions are needed due to differencnes in identifier quotes "" ``
        return InputColumn(self.col_name, self.dialect)

    @property
    def dialect_mapping(self):
        return all_dialects_mapping[self.dialect]


@dataclass
class LevenshteinLevel(ComparisonLevelCreator):
    col_name: str
    distance_threshold: int
    dialect: str = None

    def create_sql(self):
        input_col = self.input_column
        lev_fn_name = self.dialect_mapping["levenshtein"]
        return (
            f"{lev_fn_name}({input_col.col_name}_l, {input_col.col_name}_r) "
            f"<= {self.distance_threshold}"
        )

    def create_label_for_charts(self):
        return (f"Levenshtein <= {self.distance_threshold}",)
