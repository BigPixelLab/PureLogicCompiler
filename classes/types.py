from enum import Enum, auto, IntEnum
from itertools import chain
from typing import Optional

from pydantic import BaseModel


class UniqueType(Enum):
    NOT_UNIQUE = auto()
    UNIQUE = auto()
    STRICT_UNIQUE = auto()
    PRIMARY_KEY = auto()


class OnDeleteAction(Enum):
    NO_ACTION = auto()
    SET_NULL = auto()
    CASCADE = auto()


class ConnectType(IntEnum):
    # is_not_unique, is_other_table, is_optional
    SOME_TO_EVERY = 0b000  # o-
    SOME_TO_SOME_LEFT = 0b001  # oo
    EVERY_TO_SOME = 0b010  # -o
    SOME_TO_SOME_RIGHT = 0b011  # oo
    MANY_TO_EVERY = 0b100  # >-
    MANY_TO_SOME = 0b101  # >o
    EVERY_TO_MANY = 0b110  # -<
    SOME_TO_MANY = 0b111  # o<

    MANY_TO_MANY = 0b1000  # ><


class Field(BaseModel):
    name: str
    uniqueness: UniqueType
    type: str
    is_optional: bool
    default: Optional[str]
    comment: Optional[str]


class ComplexUniqueness(BaseModel):
    fields: list[str]
    uniqueness: UniqueType


class Table(BaseModel):
    pg_schema: str
    name: str
    fields: list[Field]
    complex_uniqueness: list[ComplexUniqueness]
    checks: list[str]

    @property
    def full_name(self):
        return f'{self.pg_schema}.{self.name}'

    def get_reference_pk(self) -> Field:
        pk = None

        if UniqueType.PRIMARY_KEY in [cu.uniqueness for cu in self.complex_uniqueness]:
            raise ValueError(f'Ссылки на таблицу с более чем одним pk не реализованы. '
                             f'Таблица "{self.full_name}"')

        for field in self.fields:
            if field.uniqueness != UniqueType.PRIMARY_KEY:
                continue

            if pk:
                raise ValueError(f'Ссылки на таблицу с более чем одним pk не реализованы. '
                                 f'Таблица "{self.full_name}"')

            pk = field

        if pk is None:
            raise ValueError(f'Ссылки на таблицу с без pk не реализованы. '
                             f'Таблица "{self.full_name}"')

        return pk


class ForeignKey(BaseModel):
    # Stored as strings because referenced table
    # can be non-existent at the moment of creation
    referenced_schema: str
    referenced_name: str

    referenced_table: Table = None

    containing_table: Table
    field: Field

    on_delete: OnDeleteAction


class DbSchema(BaseModel):
    foreign_keys: list[ForeignKey]
    tables: list[Table]
