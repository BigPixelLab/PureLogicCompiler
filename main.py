import re
from pprint import pprint
from typing import Optional

import yaml
from pydantic import BaseModel

"""
table:
- id: uuid
- db_name: str


field:
- id: uuid
- table_id: fk table

- is_optional: bool = false
  $: Является ли поле необязательным
- default: text? = null
- comment: text? = null


field_primary_key:
- $pk: (field_id, primary_key_id)

- field_id: fk field
- primary_key_id: fk primary_key


primary_key:
- id: uuid
- field_id: fk field


field_unique:
- $pk: (field_id, unique_id)

- field_id: fk field
- unique_id: fk unique


unique:
- id: uuid
- field_id: fk field


foreign_key:
- id: uuid
- field_id: fk field
- table_id: fk table
"""

# [schema.]table:
# - field: [pk|uq] type[?] [= default]
# (?:(pk|uq)\s+)?(\w+)(\?)?(?:\s*=\s*(.*))

# - field: fk [schema.]table[?]
# fk\s+((?:\w+\.)?\w+)(\?)?

# - $pk: (<field1>, <field2>, ...)
# - $uq: (<field1>, <field2>, ...)
# - $check: <condition>


FIELD_PATTERN = re.compile(
    r'(?: (?P<rule> pk | uq! | uq ) \s+ )? '
    r'(?P<type> \w+ ) '
    r'(?P<is_opt> \? )? '
    r'(?: \s* = \s* (?P<default> .*? ) \s* )?',
    flags=re.VERBOSE
)

FOREIGN_KEY_PATTERN = re.compile(
    r'(?: (?P<rule> pk | uq! | uq ) \s+ )? '
    r'fk \s+ '
    r'(?P<table> (?: \w+\. )? \w+ ) '
    r'(?P<is_opt> \? ) ',
    flags=re.VERBOSE
)

PK_UQ_PATTERN = re.compile(
    r'\( (?P<fields> '
    r'    \w+ '
    r'    (?: \s* , \s* \w+ )* '
    r') \)',
    flags=re.VERBOSE
)

PK_UQ_FIELDS_SEP_PATTERN = re.compile(
    r'\s* , \s*',
    flags=re.VERBOSE
)


data = """
auth.user:
- id: pk uuid
- telegram_id: uq int
  
- created_at: datetime
- created_by: str
- updated_at: datetime
- updated_by: str

- botpiska_credits: int)
- season_credits: int
- has_seasonpass: bool


auth.role:
- id: pk uuid

- alias: str
- is_actual: bool
- created_at: datetime
- created_by: str
- updated_at: datetime
- updated_by: str

- title: str


auth.user_role:
- role_id: fk auth.role
- user_id: fk auth.user
- $pk: (role_id, user_id)

- created_at: datetime
- created_by: str
- updated_at: datetime
- updated_by: str


auth.permission:
- id: pk uuid

- alias: str
- is_actual: bool
- created_at: datetime
- created_by: str
- updated_at: datetime
- updated_by: str

- title: str


auth.role_permission:
- role_id: fk auth.role
- permission_id: fk auth.permission
- $pk: (role_id, permission_id)

- created_at: datetime
- created_by: str
- updated_at: datetime
- updated_by: str

"""


class FkResolve(BaseModel):
    table_name: str
    match: re.Match


tables_data = yaml.unsafe_load(data)

tables_pk = {}
tables = {}

foreign_keys = []
comments = []

for table_name, table_data in tables_data.items():
    tables_pk[table_name] = table_pk = []
    tables[table_name] = table = []

    for attr in table_data:

        try:
            comment = attr.pop('$')
        except KeyError:
            comment = None

        if len(attr) == 0:
            raise ValueError(
                'Поле таблицы на может содержать только комментарий:\n'
                '- $: comment  # неверно\n'
                '\n'
                'Попробуйте:\n'
                '- field: int    # верно\n'
                '  $: comment\n'
            )

        if len(attr) > 1:
            raise ValueError(
                'Лишние данные в описании поля:\n'
                '- field1: int\n'
                '  field2: int  # неверно\n'
                '\n'
                'Попробуйте:\n'
                '- field1: int\n'
                '- field2: int  # верно\n'
            )

        attr_name, attr_value = next(iter(attr.items()))

        if comment and attr_name.startswith('$'):
            raise ValueError('Невозможно установить комментарий на правило')

        if comment and "'" in comment:
            raise ValueError('Комментарии не могут содержать символов "\'"')

        if comment:
            comments.append(f"""COMMENT ON COLUMN {table_name}.{attr_name} IS '{comment}';""")

        pk_uq_match = re.match(PK_UQ_PATTERN, attr_value)

        if attr_name == '$pk':

            if pk_uq_match is None:
                raise ValueError(f'Неверный формат $pk правила в описании таблицы "{table_name}"')

            fields = re.split(
                PK_UQ_FIELDS_SEP_PATTERN,
                pk_uq_match.group('fields')
            )

            table_pk.extend(fields)

            table.append(
                'PRIMARY KEY ({fields})'.format(
                    fields=pk_uq_match.group('fields')
                )
            )

            continue

        if attr_name == '$uq!':

            if pk_uq_match is None:
                raise ValueError(f'Неверный формат $uq правила в описании таблицы "{table_name}"')

            table.append(
                'UNIQUE NULLS DISTINCT ({fields})'.format(
                    fields=pk_uq_match.group('fields')
                )
            )

            continue

        if attr_name == '$uq':

            if pk_uq_match is None:
                raise ValueError(f'Неверный формат $uq правила в описании таблицы "{table_name}"')

            table.append(
                'UNIQUE NULLS NOT DISTINCT ({fields})'.format(
                    **pk_uq_match.groupdict()
                )
            )

            continue

        if attr_name == '$check':
            table.append(
                'CHECK {condition}'.format(
                    condition=attr_value
                )
            )

            continue

        if attr_name.startswith('$'):
            raise ValueError(f'Неизвестное правило "{attr_name}" в таблице "{table_name}"')

        if not re.fullmatch(r'\w+', attr_name):
            raise ValueError(f'Невозможно создать поле с именем "{attr_name}" в таблице "{table_name}"')

        if match := re.fullmatch(FIELD_PATTERN, attr_value):
            items = [attr_name, match.group('type')]

            if match.group('is_opt') is None:
                items.append('NOT NULL')

            if match.group('rule') == 'pk':
                items.append('PRIMARY KEY')
                table_pk.append(attr_name)

            if match.group('rule') == 'uq':
                items.append('UNIQUE NULLS NOT DISTINCT')

            if match.group('rule') == 'uq!':
                items.append('UNIQUE NULLS DISTINCT')

            if default := match.group('default'):
                items.append(f'DEFAULT {default}')

            table.append(' '.join(items))

        if match := re.fullmatch(FOREIGN_KEY_PATTERN, attr_value):
            foreign_keys.append(FkResolve(
                table_name=table_name,
                match=match
            ))

rendered_fk = []

for fk in foreign_keys:

    referenced_table = fk.match.group('table')
    containing_table = fk.table_name

    try:
        pks = tables_pk[referenced_table]
    except KeyError:
        raise ValueError(f'Нет описанной таблицы "{referenced_table}", чтобы на неё сослаться')

    if len(pks) > 1:
        raise ValueError('Не реализована возможнать ссылаться на таблицы с несколькими ключами')

    if len(pks) == 0:
        raise ValueError(f'Таблица "{referenced_table}" не содержит ключа, чтобы на неё сослаться')

    # items = [attr_name, match.group('type')]
    #
    # if match.group('is_opt') is None:
    #     items.append('NOT NULL')
    #
    # if match.group('rule') == 'pk':
    #     items.append('PRIMARY KEY')
    #     table_pk.append(attr_name)
    #
    # if match.group('rule') == 'uq':
    #     items.append('UNIQUE NULLS NOT DISTINCT')
    #
    # if match.group('rule') == 'uq!':
    #     items.append('UNIQUE NULLS DISTINCT')
    #
    # if default := match.group('default'):
    #     items.append(f'DEFAULT {default}')
    #
    # table.append(' '.join(items))

    rendered_fk.append(
        'ALTER TABLE {cnt_table} '
        'ADD CONSTRAINT fk_{cnt_f_table}_{field_name} '
        'FOREIGN KEY ({field_name}) REFERENCES {rfs_table} ({rfs_table_pk});'.format(
            cnt_table=containing_table,
            cnt_f_table=containing_table.replace('.', '_'),
            rfs_table=referenced_table,
            rfs_table_pk=pks[0],
            field_name=...  # TODO: fk.field_name
        )
    )

rendered_tables = []

for table_name, table_attrs in tables.items():
    rendered_tables.append(
        f'CREATE TABLE {table_name} (\n' +
        ',\n'.join(f'    {attr}' for attr in table_attrs) + '\n'
        ');'
    )

rendered = '\n\n'.join((
    *rendered_tables,
    '\n'.join(rendered_fk),
    '\n'.join(comments)
))

print(rendered)
