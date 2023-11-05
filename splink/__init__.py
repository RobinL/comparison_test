from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass


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


# By using an ABC, we explicitly enumerate the functions that need to be available
# for comparisons to work
class Dialect(ABC):
    @abstractproperty
    def name(self):
        pass

    @property
    def levenshtein_fn(self):
        raise NotImplementedError(f"No Levenshtein available :( for {type(self)}")


class DuckDBDialect(Dialect):
    @property
    def name(self):
        return "duckdb"

    @property
    def levenshtein_fn(self):
        return "levenshtein"


all_dialects = [DuckDBDialect()]


class ComparisonLevelCreator(ABC):
    def __init__(self, col_name: str, sql_dialect: str = None):
        """_summary_

        Args:
            col_name (str): _description_
            dialect (str, optional): _description_. Defaults to None.
            m_probability (_type_, optional): _description_. Defaults to None.
        """
        self.col_name = col_name
        self.sql_dialect = sql_dialect
        self.m_probability = None
        self.u_probability = None
        self.tf_adjustment_column = None
        self.tf_adjustment_weight = None
        self.tf_minimum_u_value = None
        self.is_null_level = None

    @abstractmethod
    def create_sql(self):
        pass

    @abstractmethod
    def create_label_for_charts(self):
        pass

    def get_comparison_level(self, sql_dialect=None):
        if sql_dialect:
            self.sql_dialect = sql_dialect
        return ComparisonLevel(self.create_level_dict())

    def create_level_dict(self):
        level_dict = {
            "sql_condition": self.create_sql(),
            "label_for_charts": self.create_label_for_charts(),
        }

        if self.m_probability:
            level_dict["m_probability"] = self.m_probability
        if self.u_probability:
            level_dict["u_probability"] = self.u_probability
        if self.tf_adjustment_column:
            level_dict["tf_adjustment_column"] = self.tf_adjustment_column
        if self.tf_adjustment_weight:
            level_dict["tf_adjustment_weight"] = self.tf_adjustment_weight
        if self.tf_minimum_u_value:
            level_dict["tf_minimum_u_value"] = self.tf_minimum_u_value
        if self.is_null_level:
            level_dict["is_null_level"] = self.is_null_level

        return level_dict

    @property
    def input_column(self):
        return InputColumn(self.col_name, self.dialect)

    @property
    def dialect(self):
        lookup = {f"{d.name}": d for d in all_dialects}
        try:
            return lookup[self.sql_dialect]
        except KeyError:
            raise ValueError(f"Dialect {self.sql_dialect} does not exist")

    def configure(
        self,
        *,
        m_probability: float = None,
        u_probability: float = None,
        tf_adjustment_column: str = None,
        tf_adjustment_weight: float = None,
        tf_minimum_u_value: float = None,
        is_null_level: bool = None,
    ):
        """_summary_

        Args:
            m_probability (float, optional): _description_. Defaults to None.
            u_probability (float, optional): _description_. Defaults to None.
            tf_adjustment_column (str, optional): _description_. Defaults to None.
            tf_adjustment_weight (float, optional): _description_. Defaults to None.
            tf_minimum_u_value (float, optional): _description_. Defaults to None.
            is_null_level (bool, optional): _description_. Defaults to None.
        """
        args = locals()
        del args["self"]
        for k, v in args.items():
            if v is not None:
                setattr(self, k, v)

        return self

    def __repr__(self):
        return (
            f"Comparison level generator for {self.create_label_for_charts()}. "
            "Call .get_comparison_level(sql_dialect) to instantiate "
            "a ComparisonLevel"
        )


class LevenshteinLevel(ComparisonLevelCreator):
    def __init__(self, col_name, distance_threshold, sql_dialect=None):
        super().__init__(col_name, sql_dialect=sql_dialect)
        self.distance_threshold = distance_threshold

    def create_sql(self):
        input_col = self.input_column
        lev_fn_name = self.dialect.levenshtein_fn

        return (
            f"{lev_fn_name}({input_col.col_name}_l, {input_col.col_name}_r) "
            f"<= {self.distance_threshold}"
        )

    def create_label_for_charts(self):
        return f"Levenshtein <= {self.distance_threshold}"


ll = LevenshteinLevel("name", distance_threshold=2)
ll

ll = LevenshteinLevel("name", distance_threshold=2).configure(
    u_probability=0.1,
    m_probability=0.9,
    tf_adjustment_column="name",
    tf_minimum_u_value=0.01,
)

ll
