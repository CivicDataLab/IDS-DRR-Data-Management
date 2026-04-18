"""Pydantic schema for configuration file."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Strict(BaseModel):
    """Forbid unknown keys so typos in config.toml are caught."""

    model_config = ConfigDict(extra="forbid")


class ParentSpec(_Strict):
    type: str
    # Name lookup.
    name: str | None = None
    name_field: str | None = None
    create_code: str | None = None
    create_code_template: str | None = None
    create_code_prefix_parts: int | None = Field(default=None, ge=1)
    # Code lookup.
    code_template: str | None = None
    code_prefix_parts: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _validate_lookup(self) -> "ParentSpec":
        name_lookup = self.name is not None or self.name_field is not None
        code_lookup = self.code_template is not None

        if name_lookup == code_lookup:
            raise ValueError(
                "parent must use exactly one of name lookup (`name` or "
                "`name_field`) or code lookup (`code_template`)"
            )

        if name_lookup:
            if (self.name is not None) == (self.name_field is not None):
                raise ValueError(
                    "parent must set exactly one of `name` or `name_field`"
                )
            if self.create_code is not None and self.create_code_template is not None:
                raise ValueError(
                    "parent must set at most one of `create_code` or `create_code_template`"
                )
            if self.create_code_prefix_parts is not None and self.create_code_template is None:
                raise ValueError(
                    "`create_code_prefix_parts` requires `create_code_template`"
                )
            if self.code_prefix_parts is not None:
                raise ValueError(
                    "parent must not set `code_prefix_parts` when using name lookup"
                )

        if code_lookup:
            if self.code_prefix_parts is not None and self.code_template is None:
                raise ValueError(
                    "`code_prefix_parts` requires `code_template`"
                )
            if (
                self.create_code is not None
                or self.create_code_template is not None
                or self.create_code_prefix_parts is not None
            ):
                raise ValueError(
                    "parent must not set any `create_code*` fields when using code lookup"
                )

        return self


class GeojsonSpec(_Strict):
    path: str
    geo_type: str
    name_field: str
    code_template: str
    code_prefix_parts: int | None = Field(default=None, ge=1)
    parent: ParentSpec


class StateSpec(_Strict):
    name: str
    indicators: str
    data: str
    hidden: bool = False


class ReportColumn(_Strict):
    label: str
    slug: str
    cumulative: bool


class ReportTable(_Strict):
    columns: list[ReportColumn]


class ReportChart(_Strict):
    title: str
    description: str
    chart_type: str
    x_axis_column: str
    x_axis_label: str
    y_axis_label: str
    show_legend: Literal["true", "false"]
    filter: str


class ReportSection1(_Strict):
    """Flood Risk Overview."""

    TABLE_2: ReportTable


class ReportSection2(_Strict):
    """Losses and Damages, rendered only when CHARTS is non-empty."""

    title: str
    sub_title: str
    CHARTS: list[ReportChart] = Field(default_factory=list)


class ReportSection3(_Strict):
    """Government Response, rendered only when CHARTS is non-empty."""

    title: str
    sub_title: str
    description: str
    CHARTS: list[ReportChart] = Field(default_factory=list)


class StateReport(_Strict):
    RESOURCE_ID: str = ""
    TRANSFORMED_RESOURCE_ID: str = ""
    SECTION_1: ReportSection1
    SECTION_2: ReportSection2
    SECTION_3: ReportSection3


class ReportsSpec(_Strict):
    enabled: bool = False
    states: dict[str, StateReport] = Field(default_factory=dict)


class Config(_Strict):
    whitelist_indicators: list[str] = Field(default_factory=list)
    default_time_period: str = "2024_08"
    subdistrict_types: list[str] = Field(default_factory=list)
    geojson: list[GeojsonSpec] = Field(default_factory=list)
    states: list[StateSpec] = Field(default_factory=list)
    reports: ReportsSpec = Field(default_factory=ReportsSpec)

    @field_validator("states")
    @classmethod
    def _validate_states(cls, specs: list[StateSpec]) -> list[StateSpec]:
        seen = set()
        for spec in specs:
            if spec.name in seen:
                raise ValueError(f"duplicate [[states]] name {spec.name!r}")
            seen.add(spec.name)
        return specs
