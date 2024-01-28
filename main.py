import argparse
import datetime
import os
import subprocess
import sys

from classes.database_schema_builder import DatabaseSchemaBuilder
from classes.sql_builder import PostgreSqlBuilder, DEFAULT_TYPE_TABLE


class CheckExecutionTime:
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


def setup(args):

    timer = CheckExecutionTime()

    with timer:
        print('Обновляем pip...')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])

        print('\n\nУстановка PyYaml...')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'PyYaml'])

        print('\n\nУстановка pydantic...')
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pydantic'])

    print(f'\n\nУстановка завершена за {timer.total():.03f}мс.\n')


def compile_to_ddl(args):

    if not os.path.exists(args.input):
        print('Указанный в качестве источника схемы путь не существует.')

    if os.path.isdir(args.input) and args.recursive:
        schema_files = []

        for directory, _, files in os.walk(args.input):
            schema_files.extend(
                os.path.join(directory, file)
                for file in files
            )

    elif os.path.isdir(args.input) and not args.recursive:
        schema_files = [
            path
            for path in os.listdir(args.input)
            if os.path.isfile(path)
        ]

    elif os.path.isfile(args.input):
        schema_files = [args.input]

    else:
        print('Указанный в качестве источника схемы путь не ведёт к файлу или директории')
        return

    dsb = DatabaseSchemaBuilder()
    timer = CheckExecutionTime()

    if not args.silent:
        print('Поехали!')

    with timer:
        for path in schema_files:
            if not args.silent:
                print(f'Обработка файла "{path}"...  ', end='')

            with open(path, 'rt', encoding='utf-8') as file:
                content = file.read()

            try:
                dsb.partial_load(content)
            except ValueError as error:
                if not args.silent:
                    print(f'ОШИБКА')

                print('ERROR:', *error.args)
                return

            if not args.silent:
                print(f'100%')

        schema = dsb.finalize()

    builder = PostgreSqlBuilder(schema, restrict_types_table=DEFAULT_TYPE_TABLE)
    ddl = builder.get_db_sql()

    if args.output is not None:
        with open(args.output, 'wt', encoding='utf-8') as file:
            file.write(ddl)
    else:
        print(ddl)
        print()

    if not args.silent:
        print(f'Компиляция завершена за {timer.total():.03f}мс.\n')


def parse_cmd_arguments():
    parser = argparse.ArgumentParser(
        description='Инструмент для работы со схемами баз данных, '
                    'описанных в формате PureLogic.'
    )

    subparsers = parser.add_subparsers(
        title='подкомманды',
        description='доступные подкомманды'
    )

    # Команда для установки
    setup_parser = subparsers.add_parser('setup',
                                         help='Установить необходимые пакеты')
    setup_parser.set_defaults(function=setup)

    # Комманды для компиляции
    compile_parser = subparsers.add_parser('compile',
                                           help='Конпиляция схемы в DDL')
    compile_parser.add_argument('input',
                                help='Путь к файлу или директории содержащей схему')
    compile_parser.add_argument('-o', '--output', default=None,
                                help='Путь к месту сохранения DDL скрипта. '
                                     'Если не указан, скрипт будет выведен в stdout')
    compile_parser.add_argument('-r', '--recursive', action='store_true',
                                help='Рекурсивный обход директории, содержащей схему')
    compile_parser.add_argument('-s', '--silent', action='store_true',
                                help='Не выводить информацию предназначенную для пользователя')
    compile_parser.set_defaults(function=compile_to_ddl)

    args: argparse.Namespace = parser.parse_args()

    if hasattr(args, 'function'):
        args.function(args)


if __name__ == '__main__':
    parse_cmd_arguments()


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
