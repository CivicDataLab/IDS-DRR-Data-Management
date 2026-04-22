"""Pydantic schema for configuration file."""

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
    resource_id: str = ""
    hidden: bool = False


class ChartTypeSpec(_Strict):
    chart_type: str
    state: str
    geo_type: str


class Config(_Strict):
    whitelist_indicators: list[str] = Field(default_factory=list)
    default_time_period: str = "2024_08"
    subdistrict_types: list[str] = Field(default_factory=list)
    simplify_tolerance: float = Field(default=0.003, gt=0)
    snap_to_grid_size: float = Field(default=0.0001, gt=0)
    geojson: list[GeojsonSpec] = Field(default_factory=list)
    states: list[StateSpec] = Field(default_factory=list)
    chart_types: list[ChartTypeSpec] = Field(default_factory=list)

    @field_validator("states")
    @classmethod
    def _validate_states(cls, specs: list[StateSpec]) -> list[StateSpec]:
        seen = set()
        for spec in specs:
            if spec.name in seen:
                raise ValueError(f"duplicate [[states]] name {spec.name!r}")
            seen.add(spec.name)
        return specs
