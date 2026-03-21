from sql.core.types import Types


def test_basic_types():
    # Проверяем предопределенные типы
    assert str(Types.INTEGER) == "INTEGER"
    assert str(Types.BIGINT) == "BIGINT"
    assert str(Types.TEXT) == "TEXT"


def test_type_with_args():
    # Проверяем поддержку точности (через __call__)
    assert str(Types.NUMERIC(10, 5)) == "NUMERIC(10, 5)"
    assert str(Types.TIMESTAMPTZ(3)) == "TIMESTAMP WITH TIME ZONE(3)"
    assert str(Types.VARCHAR(255)) == "VARCHAR(255)"


def test_arrays():
    # Проверяем массивы (через __getitem__)
    assert str(Types.TIME[:]) == "TIME[]"
    assert str(Types.TEXT[5]) == "TEXT[5]"
    assert str(Types.JSONB[:][:]) == "JSONB[][]"


def test_combined_args_and_arrays():
    # Проверяем и аргументы, и массивы одновременно
    assert str(Types.NUMERIC(12, 2)[:]) == "NUMERIC(12, 2)[]"
    assert str(Types.VARCHAR(100)[5]) == "VARCHAR(100)[5]"


def test_dynamic_types():
    # Проверяем работу __getattr__ в метаклассе
    assert str(Types.MY_CUSTOM_TYPE) == "MY CUSTOM TYPE"
    assert str(Types.LONGBLOB[:]) == "LONGBLOB[]"


def test_repr():
    # Проверяем, что repr возвращает строку в кавычках (как у тебя в коде)
    assert repr(Types.INTEGER) == "'INTEGER'"
