-- Полная зачистка
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Восстановление стандартных прав (на всякий случай)
GRANT ALL ON SCHEMA public TO "user";
GRANT ALL ON SCHEMA public TO public;

-- Включение расширений (нужно делать после воссоздания схемы)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1. Города с таймзонами
CREATE TABLE cities (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    timezone TEXT NOT NULL
);

INSERT INTO cities (name, timezone) VALUES 
('Москва', 'Europe/Moscow'),
('Калининград', 'Europe/Kaliningrad'),
('Владивосток', 'Asia/Vladivostok');

-- 2. Древовидные категории
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER REFERENCES categories(id) ON DELETE CASCADE,
    name TEXT NOT NULL
);

INSERT INTO categories (id, parent_id, name) VALUES 
(1, NULL, 'Электроника'),
(2, 1, 'Аксессуары'),
(3, 2, 'Гаджеты'),
(4, 2, 'Бижутерия'),
(5, NULL, 'Одежда');

-- 3. Магазины с графиком "через полночь"
CREATE TABLE shops (
    id SERIAL PRIMARY KEY,
    city_id INTEGER REFERENCES cities(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    open_at TIME NOT NULL,
    close_at TIME NOT NULL
);

INSERT INTO shops (city_id, name, open_at, close_at) VALUES 
(1, 'Мск 24/7 Гаджеты', '00:00', '23:59'),
(2, 'Клд Ночной бар', '20:00', '04:00'),   -- Через полночь
(3, 'Влд Утренний кофе', '06:00', '14:00');

-- 4. Пользователи с датами рождения
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    birth_date DATE NOT NULL, -- Используем DATE для дней рождения
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}'
);

INSERT INTO users (first_name, last_name, birth_date, tags, metadata) VALUES 
('Александр', 'Иванов', '1998-05-15', '{tech, premium}', '{"last_login": "2023-10-01"}'), -- ~25 лет
('Алексанр', 'Иванов', '1993-11-10', '{sale}', '{"points": 100}'),                      -- ~30 лет
('Алекс', 'Петров', '2004-02-20', '{tech}', '{}'),                                       -- ~19 лет
('Мария', 'Петрова', '2001-08-25', '{premium, vip}', '{"club": "gold"}');

-- 5. Продажи
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER REFERENCES shops(id) ON DELETE CASCADE,
    category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE,
    amount NUMERIC(10, 2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO sales (shop_id, category_id, amount) VALUES 
(1, 3, 5000.00), (1, 3, 2000.00),
(2, 4, 1500.00),
(3, 3, 3000.00);
