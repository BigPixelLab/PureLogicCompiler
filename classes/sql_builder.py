import re

from classes.types import Table, ForeignKey, Field, UniqueType, ComplexUniqueness, DbSchema, OnDeleteAction, Index


class SqlBuilder:
    def get_db_sql(self):
        raise NotImplementedError


class PostgreSqlBuilder(SqlBuilder):
    """ Класс, отвечающий за сборку DDL скрипта для Postgre """

    VALID_SCHEMA_PATTERN = re.compile(
        r'\w+',
        flags=re.VERBOSE
    )
    TYPE_PRECISION_PATTERN = re.compile(
        r'(?P<type> \w+ ) (?P<precision> \( \s* \d+ (?: \s* , \s* \d+ )* \s* \) )?',
        flags=re.VERBOSE
    )

    def __init__(
            self,

            db_schema: DbSchema,

            explicit_uniques: bool = False,  # Works only in Postgre 15+
            # Allows for STRICT_UNIQUE

            explicit_nulls: bool = False,
            # Adds NULL to nullable fields

            restrict_types_table: dict[str, str] = None,
            # Makes so only types from given table can be used
    ):
        self._foreign_keys = db_schema.foreign_keys
        self._indexes = db_schema.indexes
        self._tables = db_schema.tables

        self._explicit_uniques = explicit_uniques
        self._explicit_nulls = explicit_nulls

        self._restrict_types_table = restrict_types_table

    def get_drop_schema_sql(self, schema: str, validate: bool = True) -> str:
        if validate and re.fullmatch(self.VALID_SCHEMA_PATTERN, schema) is None:
            raise ValueError(f'Строка "{schema}" не является верным наименованием схемы')

        return f'DROP SCHEMA IF EXISTS {schema} CASCADE;'

    def get_create_schema_sql(self, schema: str, validate: bool = True) -> str:
        if validate and re.fullmatch(self.VALID_SCHEMA_PATTERN, schema) is None:
            raise ValueError(f'Строка "{schema}" не является верным наименованием схемы')

        return f'CREATE SCHEMA {schema};'

    def get_uniqueness_sql(self, uniqueness: UniqueType) -> str:
        if uniqueness == UniqueType.NOT_UNIQUE:
            raise ValueError('Невозможно сгенерировать SQL для типа уникальности "не уникален"')

        if uniqueness == UniqueType.PRIMARY_KEY:
            return 'PRIMARY KEY'

        items = ['UNIQUE']

        if self._explicit_uniques and uniqueness == UniqueType.STRICT_UNIQUE:
            items.append('NULLS DISTINCT')

        if self._explicit_uniques and uniqueness == UniqueType.UNIQUE:
            items.append('NULLS NOT DISTINCT')

        return ' '.join(items)

    def get_field_type(self, field: Field):
        if not self._restrict_types_table:
            return field.type

        match = re.fullmatch(self.TYPE_PRECISION_PATTERN, field.type)

        if match is None:
            raise ValueError(f'Указан неверный формат типа для поля "{field.name}"')

        _spec_type = match.group('type')

        try:
            type = self._restrict_types_table[_spec_type]
        except KeyError:
            raise ValueError(f'Неизвестный тип "{_spec_type}" поля "{field.name}"')

        return type + (match.group('precision') or '')

    def get_field_sql(self, field: Field) -> str:
        items = [
            field.name,
            self.get_field_type(field)
        ]

        if not field.is_optional:
            items.append('NOT NULL')
        elif self._explicit_nulls:
            items.append('NULL')

        if field.uniqueness != UniqueType.NOT_UNIQUE:
            items.append(
                self.get_uniqueness_sql(field.uniqueness)
            )

        if field.default and not field.is_computed:
            items.append(f'DEFAULT {field.default}')

        if field.default and field.is_computed:
            items.append(f'GENERATED ALWAYS AS ({field.default}) STORED')

        return ' '.join(items)

    def get_unique_constraint_sql(self, constraint: ComplexUniqueness) -> str:
        return (
            self.get_uniqueness_sql(constraint.uniqueness)
            + ' (' + ', '.join(constraint.fields) + ')'
        )

    def get_check_constraint_sql(self, condition: str) -> str:
        return f'CHECK ({condition})'

    def get_table_sql(self, table: Table):
        attributes = []

        attributes.extend(
            self.get_field_sql(field)
            for field in table.fields
        )

        attributes.extend(
            self.get_unique_constraint_sql(cu)
            for cu in table.complex_uniqueness
        )

        attributes.extend(
            self.get_check_constraint_sql(check)
            for check in table.checks
        )

        return (
            f'CREATE TABLE {table.full_name} (\n'
            + ',\n'.join(f'    {attr}' for attr in attributes) + '\n'
            + ');'
        )

    def get_field_comment_sql(self, table: Table, field: Field) -> str:
        return f"""COMMENT ON COLUMN {table.full_name}.{field.name} IS '{field.comment or ""}';"""

    def get_foreign_key_sql(self, fk: ForeignKey):
        ref_pk = fk.referenced_table.get_reference_pk()

        items = [
            f'ALTER TABLE {fk.containing_table.full_name} '
            f'ADD CONSTRAINT fk_{fk.containing_table.pg_schema}_{fk.containing_table.name}_{fk.field.name} '
            f'FOREIGN KEY ({fk.field.name}) REFERENCES {fk.referenced_table.full_name} ({ref_pk.name})'
        ]

        if fk.on_delete == OnDeleteAction.SET_NULL:
            items.append('ON DELETE SET NULL')
        elif fk.on_delete == OnDeleteAction.CASCADE:
            items.append('ON DELETE CASCADE')

        return ' '.join(items) + ';'

    def get_index_sql(self, index: Index):
        return f'CREATE INDEX ON {index.table.full_name} ({index.field})'

    def get_db_sql(self):
        sql_statements = []

        schemas = set(
            table.pg_schema
            for table in self._tables
        )

        sql_statements.append('\n'.join(
            self.get_drop_schema_sql(schema)
            for schema in schemas
        ))

        sql_statements.append('\n'.join(
            self.get_create_schema_sql(schema)
            for schema in schemas
        ))

        for table in self._tables:
            sql_statements.append(
                self.get_table_sql(table)
            )

            comments = '\n'.join(
                self.get_field_comment_sql(table, field)
                for field in table.fields
                if field.comment
            )

            if comments:
                sql_statements.append(comments)

        foreign_keys = '\n'.join(
            self.get_foreign_key_sql(fk)
            for fk in self._foreign_keys
        )

        if foreign_keys:
            sql_statements.append(foreign_keys)

        indexes = '\n'.join(
            self.get_index_sql(index)
            for index in self._indexes
        )

        if indexes:
            sql_statements.append(indexes)

        return '\n\n'.join(sql_statements)


DEFAULT_TYPE_TABLE = {
    'bigint': 'int8', 'int8': 'int8', 'i8': 'int8',
    'integer': 'int4', 'int': 'int4', 'int4': 'int4', 'i4': 'int4',
    'smallint': 'int2', 'int2': 'int2', 'i2': 'int2',

    'bigserial': 'serial8', 'serial8': 'serial8', 'ser8': 'serial8',
    'serial': 'serial4', 'serial4': 'serial4', 'ser4': 'serial4',
    'smallserial': 'serial2', 'serial2': 'serial2', 'ser2': 'serial2',

    'numeric': 'decimal', 'decimal': 'decimal', 'dec': 'decimal', 'money': 'decimal',

    'double': 'float8', 'float8': 'float8', 'f8': 'float8',
    'float': 'float4', 'real': 'float4', 'float4': 'float4', 'f4': 'float4',

    'boolean': 'bool', 'bool': 'bool', 'b': 'bool',

    'char': 'char', 'varchar': 'varchar',
    'text': 'text', 'string': 'text', 'str': 'text', 'url': 'text',

    'bit': 'bit', 'varbit': 'varbit',
    'bytea': 'bytea',
    'uuid': 'uuid',

    'date': 'date', 'time': 'time', 'interval': 'interval',
    'timestamp': 'timestamp', 'datetime': 'timestamp',

    'json': 'json', 'jsonb': 'jsonb', 'xml': 'xml',
}
