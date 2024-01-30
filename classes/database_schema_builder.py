import re
from typing import Optional

import yaml

from classes.types import Table, ForeignKey, ComplexUniqueness, Field, UniqueType, ConnectType, DbSchema, OnDeleteAction


class DatabaseSchemaBuilder:
    def __init__(self):
        self._foreign_keys = []
        self._tables = []

    def _parse_attribute(self, table: Table, name: str, value: str):
        pk_uq_match = re.match(PK_UQ_PATTERN, value)

        if match := re.fullmatch(UNIQUENESS_FIELD_PATTERN, name):

            uniqueness = match.group('uniqueness')

            if pk_uq_match is None:
                raise ValueError(f'Неверный формат "{uniqueness}" правила в таблице "{table.full_name}"')

            fields = re.split(
                PK_UQ_FIELDS_SEP_PATTERN,
                pk_uq_match.group('fields')
            )

            table.complex_uniqueness.append(ComplexUniqueness(
                fields=fields,
                uniqueness=UNIQUENESS_TEXT_TO_TYPE[uniqueness]  # '$pk1' -> 'pk'
            ))

            return

        if re.fullmatch(CHECK_FIELD_PATTERN, name):
            table.checks.append(value)
            return

        if name.startswith('$'):
            raise ValueError(f'Неизвестное правило "{name}" в таблице "{table.full_name}"')

        if not re.fullmatch(r'\w+', name):
            raise ValueError(f'Невозможно создать поле с именем "{name}" в таблице "{table.full_name}"')

        comment = None

        if match := re.search(COMMENT_PATTERN, value):
            comment = match.group('comment').strip()
            value = value[:match.start()].strip()

        if match := re.fullmatch(FIELD_PATTERN, value):
            if default := match.group('default'):
                default = default.strip()

            table.fields.append(Field(
                name=name,
                uniqueness=UNIQUENESS_TEXT_TO_TYPE[match.group('uniqueness')],
                type=match.group('type'),
                is_optional=match.group('is_optional') is not None,
                default=default,
                comment=comment
            ))

            return

        if match := re.fullmatch(FOREIGN_KEY_PATTERN, value):
            field = Field(
                name=name,
                uniqueness=UNIQUENESS_TEXT_TO_TYPE[match.group('uniqueness')],
                type='',  # will be overridden later
                is_optional=match.group('is_optional') is not None,
                default=None,
                comment=comment
            )

            table.fields.append(field)

            if match.group('is_cascade'):
                action = OnDeleteAction.CASCADE
            elif match.group('is_optional'):
                action = OnDeleteAction.SET_NULL
            else:
                action = OnDeleteAction.NO_ACTION

            self._foreign_keys.append(ForeignKey(
                referenced_schema=match.group('schema') or 'public',
                referenced_name=match.group('table'),
                containing_table=table,
                field=field,
                on_delete=action
            ))

            return

        raise ValueError(f'Неизвестное определение атрибута "{name}: {value}"')

    def _parse_table(self, schema: str, name: str, attributes: dict[str, str]):
        table = Table(
            pg_schema=schema,
            name=name,
            fields=[],
            complex_uniqueness=[],
            checks=[]
        )

        for attr_name, attr_value in attributes.items():
            self._parse_attribute(table, attr_name, attr_value)

        self._tables.append(table)

    def _resolve_foreign_keys(self):
        for fk in self._foreign_keys:
            try:
                table = next(
                    t for t in self._tables
                    if t.pg_schema == fk.referenced_schema
                    and t.name == fk.referenced_name
                )
            except StopIteration:
                raise ValueError(f'Невозможно разрешить внешний ключ таблицы "{fk.containing_table.full_name}", '
                                 f'т.к. таблицы "{fk.referenced_schema}.{fk.referenced_name}" не существует')

            # Throws necessary exceptions
            pk = table.get_reference_pk()

            fk.referenced_table = table
            fk.field.type = pk.type

    def _parse_schema(self, markup: dict):
        for full_table_name, table_data in markup.items():

            if full_table_name.startswith('$'):
                continue

            match: re.Match = re.match(TABLE_PATTERN, full_table_name)

            if match is None:
                raise ValueError(f'"{full_table_name}" - не является корректным названием таблицы')

            self._parse_table(
                match.group('schema') or 'public',
                match.group('table'),
                table_data
            )

    def partial_load(self, pl_schema: str):
        """
        Позволяет загружать схему базы данных частями. После загрузки всех
        частей, необходимо вызвать метод finalize()::

            schema_files = [ ... ]
            dsb = DatabaseSchemaBuilder()

            for path in schema_files:
                with open(path, 'rt', encoding='utf-8') as file:
                    dsb.partial_load(file.read())
            schema = dsb.finalize()

        """
        data = yaml.safe_load(pl_schema)
        self._parse_schema(data)

    def finalize(self) -> DbSchema:
        self._resolve_foreign_keys()
        return DbSchema(
            foreign_keys=list(self._foreign_keys),
            tables=list(self._tables)
        )

    def load(self, pl_schema: str) -> DbSchema:
        self.partial_load(pl_schema)
        return self.finalize()

    def _is_mid_in_m2m(self, table: Table) -> Optional[tuple[Table, Table]]:
        """ Является ли таблица связующей в many-to-many соединении """

        # Condition for many-to-many is that this table needs
        # to have only two connection, every on which is many-to-every.
        # pk -< fk, fk >- pk

        # There is actually many different types for many-to-many.
        # Here we checking for pure many-to-many and one with
        # allowed duplicates.
        # What it means is - we don't care if these fk are unique
        # together or not, as long as they're not optional

        foreign_keys = [
            fk for fk in self._foreign_keys
            if fk.containing_table == table
               or fk.referenced_table == table
        ]

        if len(foreign_keys) != 2:
            return None

        tables = []

        for fk in foreign_keys:
            connection: ConnectType = ConnectType(
                (fk.field.uniqueness == UniqueType.NOT_UNIQUE) * 4  # is_not_unique
                + (table == fk.referenced_table) * 2  # is_other_table
                + fk.field.is_optional  # is_optional
            )

            if connection != ConnectType.MANY_TO_EVERY:
                return None

            tables.append(fk.referenced_table)

        # noinspection PyTypeChecker
        return tuple(tables)

    def get_connections(self, table: Table):

        # is_unique, is_this_table, is_mandatory

        # uq   -> pk        o-  utm
        # uq ? -> pk        oo  ut_
        # pk <- uq          -o  u_m
        # pk <- uq ?        oo  u__
        # __   -> pk        >-  _tm
        # __ ? -> pk        >o  _t_
        # pk <- __          -<  __m
        # pk <- __ ?        o<  ___

        # pk <- fk, fk -> pk   ><

        connections = []

        # Self connections
        for fk in self._foreign_keys:

            if table == fk.containing_table:
                ref_table = fk.referenced_table
            elif table == fk.referenced_table:
                ref_table = fk.containing_table
            else:
                continue

            connection: ConnectType = ConnectType(
                (fk.field.uniqueness == UniqueType.NOT_UNIQUE) * 4  # is_not_unique
                + (table == fk.referenced_table) * 2  # is_other_table
                + fk.field.is_optional  # is_optional
            )

            mid_table = None

            if connection == ConnectType.EVERY_TO_MANY:
                if m2m_tables := self._is_mid_in_m2m(ref_table):
                    mid_table = ref_table

                    # m2m_tables contains exactly two tables,
                    # one of which is this table
                    m2m_tables = list(m2m_tables)
                    m2m_tables.remove(table)
                    ref_table = m2m_tables[0]

                    connection = ConnectType.MANY_TO_MANY

            connections.append((connection, ref_table, mid_table))

        connections.sort(key=lambda x: x[0])

        return [
            (CONNECTION_ORDER_TO_SYMBOL[o.value], tb, mtb)
            for (o, tb, mtb) in connections
        ]


CONNECTION_ORDER_TO_SYMBOL = [
    'o-', 'oo', '-o', 'oo',
    '>-', '>o', '-<', 'o<',
    '><'
]


UNIQUENESS_TEXT_TO_TYPE = {
    'pk': UniqueType.PRIMARY_KEY,
    'uq': UniqueType.UNIQUE,
    'uq!': UniqueType.STRICT_UNIQUE,
    None: UniqueType.NOT_UNIQUE
}

TABLE_PATTERN = re.compile(
    r'(?: (?P<schema> \w+ ) \. )? (?P<table> \w+ )',
    flags=re.VERBOSE
)

UNIQUENESS_FIELD_PATTERN = re.compile(
    r'\$ (?P<uniqueness> pk | uq! | uq ) (?: _ \w* )?',
    flags=re.VERBOSE
)

CHECK_FIELD_PATTERN = re.compile(
    r'\$ check (?: _ \w* )?',
    flags=re.VERBOSE
)

PK_UQ_PATTERN = re.compile(
    r'\( \s* (?P<fields> \w+ (?: \s* , \s* \w+ )* ) \s* \)',
    flags=re.VERBOSE
)

PK_UQ_FIELDS_SEP_PATTERN = re.compile(
    r'\s* , \s*',
    flags=re.VERBOSE
)

COMMENT_PATTERN = re.compile(
    r'-- (?P<comment> .* )',
    flags=re.VERBOSE
)

FIELD_PATTERN = re.compile(
    r'(?: (?P<uniqueness> pk | uq! | uq ) \s+ )? '
    r'(?P<type> \w+ (?: \( \s* \d+ (?: \s* , \s* \d+ )* \s* \) )? ) '
    r'(?P<is_optional> \? )? '
    r'(?: \s* = \s* (?P<default> .* ) \s* )?',
    flags=re.VERBOSE
)

FOREIGN_KEY_PATTERN = re.compile(
    r'(?: (?P<uniqueness> pk | uq! | uq ) \s+ )? '
    r'fk \s+ (?: (?P<schema> \w+ ) \. )? (?P<table> \w+ ) '
    r'(?: (?P<is_optional> \? ) | (?P<is_cascade> ! ) )? ',
    flags=re.VERBOSE
)
