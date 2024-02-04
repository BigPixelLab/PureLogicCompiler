from typing import Optional

import psycopg2
# noinspection PyProtectedMember
from psycopg2._psycopg import connection, cursor
from psycopg2.extras import RealDictCursor, RealDictRow

from classes.pl_types import Table, Index, ForeignKey, DbSchema, Field, OnDeleteAction


class ConnectionManager:
    def __init__(self, dsn: str, cursor_factory=None):
        self._connection_string = dsn
        self._connection: connection = psycopg2.connect(self._connection_string, cursor_factory=cursor_factory)
        self._cursor: Optional[cursor] = None

    def __enter__(self) -> cursor:
        if self._connection.closed:
            self._connection = psycopg2.connect(self._connection_string)
        self._cursor = self._connection.cursor()
        return self._cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._cursor.closed:
            self._cursor.close()


class DbInspector:
    def __init__(self, dsn: str):
        self._cm = ConnectionManager(dsn, cursor_factory=RealDictCursor)
        self._foreign_keys: list[ForeignKey] = []
        self._indexes: list[Index] = []
        self._tables: list[Table] = []
        
    def _inspect_tables(self):
        """ Получает список таблиц в базе данных """
        query = """
            WITH
            -- Колонки, содержащие комментарии
            "comments" AS (
                SELECT
                    c.table_schema, 
                    c.table_name, 
                    c.column_name, 
                    pd.description AS "comment"
                FROM pg_catalog.pg_class pc
                JOIN pg_catalog.pg_namespace pn ON pn."oid" = pc.relnamespace
                JOIN pg_catalog.pg_description pd ON pd.objoid = pc."oid"
                JOIN information_schema."columns" c ON c.ordinal_position = pd.objsubid
                    AND c.table_schema = pn.nspname
                    AND c.table_name = pc.relname
            ),
            -- Ограничения типа PRIMARY KEY и UNIQUE, сопоставленные со списком
            -- имён колонок, на которые они ссылаются
            "constraints" AS (
                SELECT 
                    ccu.table_schema, 
                    ccu.table_name,
                    array_agg(ccu.column_name) AS "columns",
                    ccu.constraint_schema, 
                    ccu.constraint_name, 
                    tc.constraint_type
                FROM information_schema.constraint_column_usage ccu
                NATURAL JOIN information_schema.table_constraints tc
                WHERE ccu.table_schema NOT IN ('pg_catalog', 'information_schema')
                    AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                GROUP BY ccu.table_schema, ccu.table_name, 
                    ccu.constraint_schema, ccu.constraint_name, 
                    tc.constraint_type
            ),
            -- Ограничения, представленные в виде JSON и сгруппированные по таблицам
            "table_constraints" AS (
                SELECT 
                    con.table_schema, 
                    con.table_name,
                    json_agg(
                        jsonb_build_object(
                            'fields', con."columns",
                            'uniqueness', con.constraint_type
                        )
                    ) AS "constraints"
                FROM "constraints" AS con
                WHERE array_length(con."columns", 1) > 1
                GROUP BY con.table_schema, con.table_name
            ),
            -- Колонки, представленные в виде JSON и сгруппированные по таблицам
            "fields" AS (
                SELECT
                    col.table_schema AS "table_schema",
                    col.table_name AS "table_name",
                    json_agg(
                        jsonb_build_object(
                            'name', col.column_name,
                            'uniqueness', CASE
                                WHEN con.constraint_type IS NULL THEN 'NOT UNIQUE'
                                ELSE con.constraint_type
                            END,
                            'type', col.data_type,
                            'is_optional', col.is_nullable = 'YES',
                            'is_computed', col.is_generated = 'ALWAYS',
                            'default', CASE 
                                WHEN col.is_generated = 'ALWAYS' THEN col.generation_expression
                                ELSE col.column_default
                            END,
                            'comment', com."comment"
                        )
                        ORDER BY col.ordinal_position
                    ) AS "fields"
                FROM information_schema."columns" AS col
                LEFT JOIN "comments" AS com USING (table_schema, table_name, column_name)
                LEFT JOIN "constraints" AS con ON 
                    con.table_schema = col.table_schema
                    AND con.table_name = col.table_name
                    AND array_length(con."columns", 1) = 1
                    AND con."columns"[1] = col.column_name
                WHERE col.table_schema NOT IN ('pg_catalog', 'information_schema')
                GROUP BY col.table_schema, col.table_name
            ),
            -- Ограничения типа CHECK
            "checks" AS (
                SELECT
                    tc.table_schema,
                    tc.table_name,
                    cc.check_clause
                FROM information_schema.check_constraints cc
                JOIN information_schema.constraint_column_usage ccu 
                    USING (constraint_schema, constraint_name)
                JOIN information_schema.table_constraints tc
                    USING (constraint_schema, constraint_name)
                GROUP BY tc.table_schema, tc.table_name, 
                    constraint_schema, constraint_name, 
                    cc.check_clause
            ),
            -- Ограничения типа CHECK, собранные в JSON и сгруппированные
            -- по таблицам
            "table_checks" AS (
                SELECT
                    c.table_schema,
                    c.table_name,
                    json_agg(c.check_clause) AS "checks"
                FROM "checks" AS c
                GROUP BY c.table_schema, c.table_name
            )
            -- Таблицы, собранные в JSON
            SELECT
                json_build_object(
                    'pg_schema', t.table_schema,
                    'name', t.table_name,
                    'fields', CASE
                        WHEN f.fields IS NULL THEN '[]'::json
                        ELSE f.fields
                    END,
                    'complex_uniqueness', CASE
                        WHEN con."constraints" IS NULL THEN '[]'::json
                        ELSE con."constraints"
                    END,
                    'checks', CASE
                        WHEN tch.checks IS NULL THEN '[]'::json
                        ELSE tch.checks
                    END
                ) "table"
            FROM information_schema."tables" AS t
            LEFT JOIN "fields" AS f USING (table_schema, table_name)
            LEFT JOIN "table_checks" AS tch USING (table_schema, table_name)
            LEFT JOIN "table_constraints" AS con ON
                con.table_schema = t.table_schema
                AND con.table_name = t.table_name
            WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
                AND t.table_type = 'BASE TABLE'
        """

        with self._cm as _cursor:
            _cursor.execute(query)
            tables_data: list[RealDictRow] = _cursor.fetchall()

        self._tables = []
        for table_data in tables_data:
            self._tables.append(Table(**table_data['table']))

    def get_table(self, schema: str, name: str) -> Table:
        try:
            return next(
                table for table in self._tables
                if table.pg_schema == schema
                and table.name == name
            )
        except StopIteration:
            raise ValueError(f"Таблица не найдена: {schema}.{name}")

    @classmethod
    def get_table_field(cls, table: Table, field_name: str) -> Field:
        try:
            return next(
                field for field in table.fields
                if field.name == field_name
            )
        except StopIteration:
            raise ValueError(f"Поле не найдено: {field_name}")

    def _inspect_foreign_keys(self):
        """ Получает список внешних ключей в базе данных. Должно вызываться
           только после _inspect_tables! """
        query = """
            SELECT
                kcu.table_schema "containing_schema",
                kcu.table_name "containing_name",
                (array_agg(kcu.column_name))[1] "containing_column",
                ccu.table_schema "referenced_schema",
                ccu.table_name "referenced_name",
                (array_agg(ccu.column_name))[1] "referenced_column",
                rc.delete_rule "on_delete"
            FROM information_schema.referential_constraints rc
            JOIN information_schema.constraint_column_usage ccu
                USING (constraint_schema, constraint_name)
            JOIN information_schema.key_column_usage kcu
                USING (constraint_schema, constraint_name)
            -- Fk может ссылаться на насколько колонок, такие fk мы
            -- не поддерживаем
            GROUP BY kcu.table_schema, kcu.table_name, 
                ccu.table_schema, ccu.table_name, 
                rc.delete_rule
            HAVING count(1) = 1
        """

        with self._cm as _cursor:
            _cursor.execute(query)
            fks_data: list[RealDictRow] = _cursor.fetchall()

        self._foreign_keys = []
        for fk_data in fks_data:
            containing_table = self.get_table(fk_data["containing_schema"], fk_data["containing_name"])
            referenced_table = self.get_table(fk_data["referenced_schema"], fk_data["referenced_name"])

            # Внешний ключ ссылается не на ключевое поле таблицы. Такие ссылки мы пока
            # не поддерживаем
            if fk_data["referenced_column"] != referenced_table.get_reference_pk().name:
                continue

            self._foreign_keys.append(ForeignKey(
                referenced_schema=referenced_table.pg_schema,
                referenced_name=referenced_table.name,
                referenced_table=referenced_table,
                containing_table=containing_table,
                field=self.get_table_field(containing_table, fk_data["containing_column"]),
                on_delete=OnDeleteAction(fk_data["on_delete"])
            ))

    def inspect(self) -> DbSchema:
        self._inspect_tables()
        self._inspect_foreign_keys()
        return DbSchema(
            foreign_keys=self._foreign_keys,
            tables=self._tables,
            indexes=[]
        )
