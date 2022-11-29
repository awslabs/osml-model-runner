from dataclasses import dataclass, field
from enum import auto
from inspect import signature
from typing import Any, Dict, List, Optional, Tuple

from .auto_string_enum import AutoStringEnum
from .exceptions import InvalidClassificationException


def classification_asdict_factory(attributes: List[Tuple[str, Any]]) -> dict:
    """
    Factory method designed to be used with asdict(<Classification object>, dict_factory=classification_asdict_factory)
    Removes attributes with None values from the resulting dictionary.
    :param attributes: dataclass asdict object
    :return: asdict representation of object with None attributes removed
    """
    return {k: v for (k, v) in attributes if v is not None}


class ClassificationLevel(str, AutoStringEnum):
    """
    Enum defining the 4 security classification levels.
    """

    UNCLASSIFIED = auto()
    CONFIDENTIAL = auto()
    SECRET = auto()
    TOP_SECRET = "TOP SECRET"


@dataclass(frozen=True)
class Classification:
    """
    Dataclass for holding security classification information.  Level, caveats, and releasability are provided at
    init and the classification string is generated automatically.  If an invalid classification is provided an
    InvalidClassificationException will be thrown.

    :param level: str enum - security classification level defined by enum
    :param caveats: List[str] - list of security caveats
    :param releasability: str - defining the releasability of the information
    """

    classification: str = field(init=False)
    level: Optional[ClassificationLevel] = None
    caveats: Optional[List[str]] = None
    releasability: Optional[str] = None

    def __post_init__(self):
        if self.releasability is not None:
            object.__setattr__(self, "releasability", self.releasability.upper())
        if self.caveats is not None:
            object.__setattr__(self, "caveats", [caveat.upper() for caveat in self.caveats])

        classification = None
        if self.level == ClassificationLevel.UNCLASSIFIED and self.caveats is None:
            if self.releasability is not None:
                classification = f"{self.level.value}//{self.releasability}"
            else:
                classification = str(self.level.value)
        elif (
            self.level is not None
            and self.level != ClassificationLevel.UNCLASSIFIED
            and self.releasability is not None
        ):
            if self.caveats is not None:
                caveats_string = "/".join(self.caveats)
                classification = f"{self.level.value}//{caveats_string}//{self.releasability}"
            else:
                classification = f"{self.level.value}//{self.releasability}"

        if classification:
            object.__setattr__(self, "classification", classification)
        else:
            raise InvalidClassificationException(
                f"Invalid classification. {self.level}|{self.caveats}|{self.releasability}"
            )

    @classmethod
    def from_dict(cls, params: dict):
        new_params: Dict[str, Any] = {}
        for k, v in params.items():
            if k in signature(cls).parameters:
                if k == "level" and not isinstance(v, ClassificationLevel):
                    new_params[k] = ClassificationLevel[v.replace(" ", "_")]
                else:
                    new_params[k] = v
        return cls(**new_params)
