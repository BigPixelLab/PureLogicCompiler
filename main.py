import argparse
import datetime
import os
from itertools import chain
from typing import Optional, Iterable

from classes.database_schema_builder import DatabaseSchemaBuilder
from classes.sql_builder import PostgreSqlBuilder, DEFAULT_TYPE_TABLE
from classes.types import DbSchema


class ExecutionTimeContextManager:
    """ Класс позволяющий замерить время выполнения блока кода,
       находящегося в контекстном менеджере """

    def __init__(self):
        self._start_time = None
        self._end_time = None
        self._total_time = None

    def __enter__(self):
        self._start_time = datetime.datetime.now()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._end_time = datetime.datetime.now()
        self._total_time = self._end_time - self._start_time

    def total(self):
        return self._total_time.total_seconds() * 10**3


class Application:
    def __init__(self):
        self._args: Optional[argparse.Namespace] = None

    def notify(self, *args, sep: str = ' ', end: str = '\n'):
        """ Метод для вывода оповещений пользователю """

        if getattr(self._args, 'silent', False):
            return
        print(*args, sep=sep, end=end)

    def output(self, *args, sep: str = ' ', end: str = '\n', create_file: bool = False):
        """ Метод для вывода результата работы программы """

        filename = getattr(self._args, 'output', None)
        encoding = getattr(self._args, 'encoding', 'utf-8')
        mode = ['at', 'xt'][create_file]

        if filename is None and create_file:
            os.system('cls')

        if filename is None:
            print(*args, sep=sep, end=end)
            return

        with open(filename, mode, encoding=encoding) as file:
            print(*args, sep=sep, end=end, file=file)

    def error(self, exception: Exception):
        """ Метод для вывода сообщений об ошибке """

        print('ERROR:', *exception.args)

    def _load_schema(self, schema_files: Iterable[str], encoding: str) -> Optional[DbSchema]:
        dsb = DatabaseSchemaBuilder(add_fk_indexes=self._args.add_fk_index)

        self.notify('ЗАГРУЗКА СХЕМЫ:')
        for path in schema_files:
            self.notify(f'Обработка файла "{path}"...  ', end='')

            with open(path, 'rt', encoding=encoding) as file:
                content = file.read()

            try:
                dsb.partial_load(content)
            except ValueError as error:
                self.notify(f'ОШИБКА')
                self.error(error)
                return

            self.notify(f'100%')

        schema = dsb.finalize()
        self.notify('готово.')

        return schema

    def _generate_ddl(self, schema: DbSchema) -> Optional[str]:
        self.notify('\nГЕНЕРАЦИЯ DDL:')
        builder = PostgreSqlBuilder(schema, restrict_types_table=DEFAULT_TYPE_TABLE)

        try:
            ddl = builder.get_db_sql()
        except ValueError as error:
            self.error(error)
            return

        self.notify('готово.')

        return ddl

    def compile_to_ddl(self):
        encoding = getattr(self._args, 'encoding', 'utf-8')

        try:
            schema_files = get_files(
                self._args.input,
                recursive=not self._args.non_recursive
            )
        except Exception as error:
            print('ERROR:', *error.args)
            return

        with (timer := ExecutionTimeContextManager()):
            if (schema := self._load_schema(schema_files, encoding=encoding)) is None:
                return

            if (ddl := self._generate_ddl(schema)) is None:
                return

        self.output(ddl, create_file=True)
        self.notify(f'Компиляция завершена за {timer.total():.03f}мс.\n')

    def run(self):
        parser = argparse.ArgumentParser(
            description='Инструмент для работы со схемами баз данных, '
                        'описанных в формате PureLogic.'
        )

        subparsers = parser.add_subparsers(
            title='подкомманды',
            description='доступные подкомманды'
        )

        # Комманды для компиляции
        compile_parser = subparsers.add_parser(
            'compile',
            help='Конпиляция схемы в DDL'
        )
        compile_parser.set_defaults(function=self.compile_to_ddl)

        compile_parser.add_argument(
            'input',
            help='Путь к файлу или директории содержащей схему'
        )
        compile_parser.add_argument(
            '-s', '--silent', action='store_true',
            help='Не выводить информацию предназначенную для пользователя'
        )

        loading_group = compile_parser.add_argument_group(
            'Загрузка схемы', 'Настройка того как загружается схема'
        )

        loading_group.add_argument(
            '-o', '--output', default=None,
            help='Путь к месту сохранения DDL скрипта. '
                 'Если не указан, скрипт будет выведен в stdout'
        )
        loading_group.add_argument(
            '--encoding', default=None,
            help='Кодировка входных файлов. Если указан output, '
                'произведённый файл будет в этой же кодировке. '
                'По-умолчанию используется "utf-8"'
        )
        loading_group.add_argument(
            '--non-recursive', action='store_true', dest='non_recursive',
            help='Не обходить директорию рекурсивно. Если указанно, '
                 'не будет искать файлы схемы в дочених папках'
        )

        compiling_group = compile_parser.add_argument_group(
            'Генерация DDL', 'Настройка получаемого на выходе DDL'
        )

        compiling_group.add_argument(
            '--fk-index', action='store_true', dest='add_fk_index',
            help='Добавить индексы на внешние ключи'
        )

        self._args: argparse.Namespace = parser.parse_args()

        if hasattr(self._args, 'function'):
            self._args.function()


def get_files(path: str, recursive: bool = True):
    """ Возвращает список файлов, лежащих по указанному пути """

    if not os.path.exists(path):
        raise FileNotFoundError('Указанный в качестве источника схемы путь не существует.')

    if os.path.isfile(path):
        return [path]

    if not os.path.isdir(path):
        raise OSError('Указанный в качестве источника схемы путь не ведёт к файлу или директории')

    if not recursive:
        # Получаем содержимое директории с помощью listdir, клеим к каждому
        # элементу базовый путь, фильтруем то что файл
        return list(
            filter(os.path.isfile, (
                os.path.join(path, file)
                for file in os.listdir(path)
            ))
        )

    # Просим os.walk рекурсивно выдавать директории и файлы в них, к файлам
    # клеим путь директории, выпрямляем всё в сплошной список
    return list(
        chain(*[
            [os.path.join(directory, file) for file in files]
            for (directory, _, files) in os.walk(path)
        ])
    )


if __name__ == '__main__':
    Application().run()


# table = dsb.tables[1]
# connections = dsb.get_connections(table)
#
# print(table.full_name)
# for symbol, ref_table, mid_table in connections:
#     items = [symbol, ref_table.full_name]
#
#     if mid_table:
#         items.append(f' ({mid_table.full_name})')
#
#     print(' '.join(items))
