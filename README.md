# PureLogic compiler

## Как использовать

Для вывода результата в консоль:

`python .\main.py compile <input>`

Для вывода в файл:

`python .\main.py compile <input> --output <output>`

`python .\main.py compile <input> -o <output>`

Можно добавить флаги:
* `--recursive` или `-r` - если в input указан путь к директории (а не к файлу), рекурсивно пройдётся вглубь по директории и соберёт все файлы в одну схему
* `--silent` или `-s` - не будет выводить пользовательских сообщений. Если вывод схемы производится в консоль - выведет только схему, без сторонних сообщений

## Синтаксис PureLogic

```yaml
[<schema>.]<table>:
  # Обычное поле
  <field>: [ pk|uq|uq! ] <type>[?] [ = <default> | := <computed>] [ -- <comment>]
  # pk, uq и uq! указвыют на уникальность поля
  # - pk - PRIMARY KEY
  # - uq - UNIQUE
  # - uq! - Строгое UNIQUE (запрет на несколько значений NULL. Только для Postgre 15+)
  # type - Тип поля. Любой родной для Postgre тип, главное чтоб был одним словом
  # ? - Наличие указывает на то что поле необязательное
  # = <default> - Sql выражение, используемое в качестве значения по-умолчанию для поля
  # -- <comment> - Комментарий к полю. Записывается в базу данных

  # Внешний ключ
  <field>: [ pk|uq|uq! ] fk [<schema>.]<table>[?|!] [ -- <comment>]
  # [<schema>.]<table> - Таблица на которую ссылается поле

  # Уникальность по нескольким полям
  $<pk|uq|uq!>[_<name>]: (<field1>[, <field2>[, ...])
  # _<name> - Если в таблице содержится несколько наборов уникальных полей,
  #   возникает проблема с дублированием ключей, чтобы этого избежать
  #   добавляется имя

  # Проверка условия
  $check[_<name>]: (<condition>)
```

### Описание таблиц

Таблицы можно объявлять как с указанием схемы, так и без.
Если схема не указана, по умолчанию используется public.

```yaml
my_table1:
  id: serial

public.my_table2:
  id: serial

my_schema.my_table3:
  id: serial
```

### Описание полей

```yaml
table:
  # Обычное поле. Ключом указывается имя поля, значением - его тип.
  # Установить можно любой PostgreSQL тип, состоящий из одного слова
  # (все многословные типы в PostgreSQL имеют однословный синоним)
  basic_field: int

  # По-умолчанию все поля устанавливаются как NOT NULL, т.е. обязательные,
  # но их можно сделать и не обязательными, добавив к типу '?'
  optional_field: int?

  # Ключевые и уникальные поля можно следать следующим образом:
  unique_field: uq int
  strict_unique_field: uq! int  # PG 15+ Разрешает максимум 1 NULL
  primary_key_field: pk int

  # Одно поле не может одновременно иметь несколько типов уникальности.

  # DEFAULT. Есть возможность давать полям значения по-умолчанию.
  # Всё после знака '=' до конца строки (или до '--', если есть) 
  # будет вставлено в генерируемый SQL
  default_field: int = 5

  # GENERATED. Или генерировать значение поля на основе других полей.
  # Всё после знака ':=' до конца строки (или до '--', если есть) 
  # будет вставлено в генерируемый SQL
  generated_field: int := default_field + basic_field

  # Поле не может быть объявлено одновременно как DEFAULT и как GENERATED.

  # Полям можно задать комментарии, которые будут добавлены в базу.
  # Всё после '--' до конца строки станет комментарием. Пробелы в начале
  # и в конце комментария удаляются.
  commented_field: int  -- Hello world!
```

### Описание внешних ключей

```yaml
table:
  # Поле можно указать как содержащее внешний ключ, если написать 'fk'.
  # fk-поля имеют несколько особенностей:
  # - вместо типа в них указывается таблица, на которую ссылается поле
  # - fk-поля не могут иметь значений по-умолчанию или быть генерируемыми
  fk_field: fk another_table

  # Таблицы в fk-полях также могут иметь или не иметь явно заданную схему
  # По-умолчанию используется схема 'public'
  fk_field_2: fk my_schema.third_table

  # Уникальность fk-поля можно установить так же как и для любого другого поля.
  # Тип уникальности должен обязательно стоять перед 'fk'.
  pk_fk_field: pk fk another_table

  # Необязательность указывается знаком '?' стоящим после таблицы
  optional_fk_field: fk another_table?

  # Если fk-поле объявлено как обязательное, то при удалении записи, на которую
  # идёт ссылка - будет генерироваться исключение. ON DELETE NO ACTION
  no_action_fk: fk some_table

  # fk-поля отмеченные как необязательные в этой ситуации будут получать значение
  # NULL. ON DELETE SET NULL
  set_null_fk: fk some_table?

  # Чтобы сделать так, чтобы запись этой таблицы удалялась вместе с записью в
  # таблице на которую идёт ссылка, можно использовать символ '!'. ON DELETE CASCADE
  cascade_fk: fk some_table!
  # Каскадные необязательные поля не поддерживаются форматом
```

### Описание ограничений. CONSTRAINT

```yaml
table:
  # Если уникальность устанавливается на одно поле, её можно указать прямо там,
  # но если на несколько, то здесь выйти из положения поможет такая запись:
  $pk: [field1, field2, ...]
  $uq: [field1, field2, ...]
  $uq!: [field1, field2, ...]

  # Также на таблицу можно поставить ограничение CHECK, следующим образом:
  $check: field1 != field2
  # В качестве значения пишется условие, которое в таком же виде будет вставлено
  # при компиляции в SQL

  # Если одинаковых типов ограничений в таблице несколько, им можно дать имена.
  # Имена должны быть разделены с типом ограничения символом '_'
  $check_another: true
  $pk_2: [field1]
```

## Примеры

```yaml
public.library:
  id: pk uuid = gen_random_uuid()  -- id библиотеки
  # Поле 'id', типа 'uuid', являющееся ключевым
  # и имеющее в качестве значения по-умолчанию
  # gen_random_uuid()

  name: text  -- Название библиотеки

public.book:
  id: pk uuid = gen_random_uuid()
  library_id: fk public.library?
```

После компиляции сгенерирует:

```sql
DROP SCHEMA IF EXISTS public CASCADE;

CREATE SCHEMA public;

CREATE TABLE public.library (
    id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL
);

COMMENT ON COLUMN public.library.id IS 'id библиотеки';
COMMENT ON COLUMN public.library.name IS 'Название библиотеки';

CREATE TABLE public.book (
    id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    library_id uuid
);

ALTER TABLE public.book ADD CONSTRAINT fk_public_book_library_id FOREIGN KEY (library_id) REFERENCES public.library (id) ON DELETE SET NULL;
```
