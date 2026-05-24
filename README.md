# DistributedOrderQueue
Простая система для асинхронной обработки заказов: HTTP‑запросы от клиентов попадают в собственную очередь, а затем обрабатываются воркерами с проверкой оплаты через отдельный платёжный сервис.
## Что нужно для запуска
- PostgreSQL (можно на том же компьютере или отдельно)
- Любой современный браузер (для веб‑интерфейса)
## Структура проекта
- RestApi — принимает заказы и публикует их в очередь
- MessageBroker — очередь сообщений (TCP‑сервер)
- OrderWorker — обработчик заказов (можно запустить несколько экземпляров)
- CardService — платёжный сервис (TCP‑сервер со своей базой)
- StressTest — консольная утилита для нагрузочного тестирования
- Shared — общая библиотека
## Создание бд
В проекте две базы данных одна наша, а другая как будто не наша, а внешняя, но так как мы не можем подключится к реальным платежным системах мы создаём сами её.
В первую очередь требуется создать две базы данных: postgres и carddb. Далее в них нужно выполнить такие команды:
### Для postgres
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash BYTEA NOT NULL,
    salt BYTEA NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price NUMERIC NOT NULL
);

INSERT INTO products (name, price) VALUES
('Футболка', 19.99),
('Джинсы', 49.99),
('Кроссовки', 79.99),
('Кепка', 14.99),
('Рюкзак', 39.99),
('Часы', 99.99),
('Наушники', 29.99),
('Зарядное устройство', 9.99),
('Книга', 12.99),
('Кофе', 5.99)
ON CONFLICT (product_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    status VARCHAR(20) NOT NULL DEFAULT 'accepted',
    published_to_queue BOOLEAN NOT NULL DEFAULT FALSE,
    items JSONB NOT NULL,
    delivery_type VARCHAR(50) NOT NULL DEFAULT 'standard',
    address TEXT NOT NULL,
    card_number VARCHAR(19) DEFAULT '',

### Для carddb

CREATE TABLE IF NOT EXISTS cards (
    user_id INTEGER PRIMARY KEY,
    card_number TEXT NOT NULL,
    balance NUMERIC NOT NULL DEFAULT 10000.00
);
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
