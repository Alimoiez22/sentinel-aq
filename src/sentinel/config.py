"""Typed loading of config/scope.yaml. A typo or a wrong type fails loudly at start-up."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

# Files appear about 72 h after the local day ends (RESEARCH §5.1); 4 days back is safe.
ARRIVAL_MARGIN_DAYS = 4


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Point(_Strict):
    """A WGS 84 point with a search radius."""

    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    radius_km: float = Field(gt=0, le=25)  # API maximum radius is 25 km


class DateRange(_Strict):
    """Inclusive range of source dates. `end=None` means today minus the arrival margin."""

    start: date
    end: date | None = None

    def resolved_end(self, today: date | None = None) -> date:
        """The concrete last date."""
        if self.end is not None:
            return self.end
        today = today or datetime.now(UTC).date()
        return today - timedelta(days=ARRIVAL_MARGIN_DAYS)

    @model_validator(mode="after")
    def _ordered(self) -> DateRange:
        if self.end is not None and self.end < self.start:
            raise ValueError("date_range.end is before date_range.start")
        return self


class Scope(_Strict):
    """Everything config/scope.yaml may contain."""

    version: int = Field(ge=1, le=1)
    country_iso: str = Field(pattern=r"^[A-Z]{2}$")
    lahore: Point
    date_range: DateRange
    parameters: list[str] = Field(min_length=1)
    locations: list[int] = Field(default_factory=list)
    snapshot_locations: list[int] = Field(default_factory=list)


def load_scope(path: Path = Path("config/scope.yaml")) -> Scope:
    """Read and validate the scope file."""
    with path.open(encoding="utf-8") as f:
        return Scope.model_validate(yaml.safe_load(f))
