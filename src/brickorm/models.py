from sqlmodel import SQLModel, Field, Relationship
from typing import List, Optional, ClassVar, Self
import requests
from pydantic import model_validator
import gzip
import io
import pandas as pd
from sqlalchemy import PrimaryKeyConstraint
from enum import StrEnum


class RebrickableModel(SQLModel, table=False):
    URL: ClassVar[str]

    @classmethod
    def download_instances(cls) -> list[Self]:
        r = requests.get(cls.URL, allow_redirects=True)
        if not r.ok:
            raise
        with gzip.open(io.BytesIO(r.content), "rb") as f:
            df = pd.read_csv(f)

            models = []

            for _, row in df.iterrows():
                d = {
                    k: (v if not pd.isna(v) else None) for k, v in row.to_dict().items()
                }
                models.append(cls.model_validate(d))

            return models


class PartCategory(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/part_categories.csv.gz"

    id: int = Field(default=None, primary_key=True)
    name: str = Field(max_length=200)

    parts: List["Part"] = Relationship(back_populates="category")


class Part(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/parts.csv.gz"

    part_num: str = Field(primary_key=True, max_length=20)
    name: str = Field(max_length=250)
    part_cat_id: int = Field(foreign_key="partcategory.id")

    category: Optional[PartCategory] = Relationship(back_populates="parts")
    elements: List["Element"] = Relationship(back_populates="part")


class Color(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/colors.csv.gz"

    id: int = Field(default=None, primary_key=True)
    name: str = Field(max_length=200)
    rgb: str = Field(max_length=6)
    is_trans: bool

    elements: List["Element"] = Relationship(back_populates="color")


class Element(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/elements.csv.gz"

    element_id: str = Field(primary_key=True, max_length=10)
    part_num: str = Field(foreign_key="part.part_num", max_length=20)
    color_id: int = Field(foreign_key="color.id")

    part: Optional[Part] = Relationship(back_populates="elements")
    color: Optional[Color] = Relationship(back_populates="elements")

    @model_validator(mode="before")
    def convert_id(cls, values):
        values["element_id"] = str(values["element_id"])
        return values


class PartRelationship(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/part_relationships.csv.gz"

    class PartType(StrEnum):
        PRINT = "P"
        SUB_PART = "B"
        MOLD = "M"
        ALTERNATE = "A"
        PAIR = "R"
        PATTERN = "T"


    rel_type: PartType = Field(max_length=1)
    child_part_num: str = Field(foreign_key="part.part_num", max_length=20)
    parent_part_num: str = Field(foreign_key="part.part_num", max_length=20)

    __table_args__ = (
        PrimaryKeyConstraint("rel_type", "child_part_num", "parent_part_num"),
    )


class Minifig(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/minifigs.csv.gz"

    fig_num: str = Field(primary_key=True, max_length=20)
    name: str = Field(max_length=256)
    num_parts: int


class Set(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/sets.csv.gz"

    set_num: str = Field(primary_key=True, max_length=20)
    name: str = Field(max_length=256)
    year: int
    theme_id: int = Field(foreign_key="theme.id")
    num_parts: int

    theme: Optional["Theme"] = Relationship(back_populates="sets")
    inventories: List["Inventory"] = Relationship(back_populates="set")


class Theme(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/themes.csv.gz"

    id: int = Field(default=None, primary_key=True)
    name: str = Field(max_length=256)
    parent_id: Optional[int] = Field(foreign_key="theme.id")

    sets: List[Set] = Relationship(back_populates="theme")


class Inventory(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/inventories.csv.gz"

    id: int = Field(default=None, primary_key=True)
    version: int
    set_num: str = Field(foreign_key="set.set_num", max_length=20)

    set: Optional[Set] = Relationship(back_populates="inventories")
    inventory_parts: List["InventoryPart"] = Relationship(back_populates="inventory")
    inventory_minifigs: List["InventoryMinifig"] = Relationship(
        back_populates="inventory"
    )
    inventory_sets: List["InventorySet"] = Relationship(back_populates="inventory")


class InventoryPart(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/inventory_parts.csv.gz"

    id: int = Field(default=None, primary_key=True)
    inventory_id: int = Field(foreign_key="inventory.id")
    part_num: str = Field(foreign_key="part.part_num", max_length=20)
    color_id: int = Field(foreign_key="color.id")
    quantity: int
    is_spare: bool

    inventory: Optional[Inventory] = Relationship(back_populates="inventory_parts")


class InventoryMinifig(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/inventory_minifigs.csv.gz"

    id: int = Field(default=None, primary_key=True)
    inventory_id: int = Field(foreign_key="inventory.id")
    fig_num: str = Field(foreign_key="minifig.fig_num", max_length=20)
    quantity: int

    inventory: Optional[Inventory] = Relationship(back_populates="inventory_minifigs")


class InventorySet(RebrickableModel, table=True):
    URL = "https://cdn.rebrickable.com/media/downloads/inventory_sets.csv.gz"

    inventory_id: int = Field(foreign_key="inventory.id", primary_key=True)
    set_num: str = Field(foreign_key="set.set_num", primary_key=True, max_length=20)
    quantity: int

    inventory: Optional[Inventory] = Relationship(back_populates="inventory_sets")


ALL_MODELS = (
    PartCategory,
    Theme,
    Part,
    Color,
    Set,
    Element,
    Minifig,
    Inventory,
    InventoryPart,
    InventoryMinifig,
    InventorySet,
    PartRelationship,
)
