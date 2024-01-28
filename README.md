# PureLogic compiler

## Синтаксис PureLogic

```yaml
[<schema>.]<table>:
  # Обычное поле
  <field>: [ pk|uq|uq! ] <type>[?] [ = <default>] [ -- <comment>]
  # pk, uq и uq! указвыют на уникальность поля
  # - pk - PRIMARY KEY
  # - uq - UNIQUE
  # - uq! - Строгое UNIQUE (запрет на несколько значений NULL. Только для Postgre 15+)
  # type - Тип поля. Любой родной для Postgre тип, главное чтоб был одним словом
  # ? - Наличие указывает на то что поле необязательное
  # = <default> - Sql выражение, используемое в качестве значения по-умолчанию для поля
  # -- <comment> - Комментарий к полю. Записывается в базу данных

  # Внешний ключ
  <field>: [ pk|uq|uq! ] fk [<schema>.]<table>[?] [ -- <comment>]

  # Уникальность по нескольким полям
  $<pk|uq|uq!>[_<name>]: (<field1>[, <field2>[, ...])

  # Проверка условия
  $check[_<name>]: (<condition>)
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

ALTER TABLE public.book ADD CONSTRAINT fk_public_book_library_id FOREIGN KEY (library_id) REFERENCES public.library (id);
```
