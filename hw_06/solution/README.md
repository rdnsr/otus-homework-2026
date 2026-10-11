# TicketHub — система продажи билетов на концерты

Курсовой проект: проектирование высоконагруженной системы обработки пользовательских запросов.

**Тема:** система управления мероприятиями — билеты на концерты.

Условие и формат сдачи — [Задание.md](Задание.md).

## Состав проекта

| Раздел | Файл |
|---|---|
| 1. Требования (ФТ, НФТ, риски, backlog) | [docs/01-requirements.md](docs/01-requirements.md) |
| 2. Концептуальная архитектура (C4, стиль, обоснование) | [docs/02-architecture.md](docs/02-architecture.md) |
| 3. Сайзинг (RPS, хранилище, bandwidth, стоимость) | [docs/03-sizing.md](docs/03-sizing.md) |
| 4. Хранение данных (БД, шардирование, кэш) | [docs/04-data-storage.md](docs/04-data-storage.md) |
| 5. Взаимодействие сервисов и API-контракты | [docs/05-api-contracts.md](docs/05-api-contracts.md) |
| 6. Надёжность (RTO/RPO, репликация, DR) | [docs/06-reliability.md](docs/06-reliability.md) |
| 7. Безопасность | [docs/07-security.md](docs/07-security.md) |
| 8. Observability (метрики, алерты) | [docs/08-observability.md](docs/08-observability.md) |
| 9. Тестирование (нагрузочное, chaos) | [docs/09-testing.md](docs/09-testing.md) |

Каркас сдачи ДЗ — [Решение.md](Решение.md).

## ADR (Architecture Decision Records)

| ADR | Решение |
|---|---|
| [ADR-001](adr/ADR-001-seat-reservation.md) | Резервирование мест: Redis hold с TTL + подтверждение в PostgreSQL |
| [ADR-002](adr/ADR-002-microservices-event-driven.md) | Микросервисная event-driven архитектура вместо монолита |
| [ADR-003](adr/ADR-003-async-payment-saga.md) | Асинхронная оплата через Saga + Transactional Outbox |
| [ADR-004](adr/ADR-004-seatmap-template-library.md) | Схема зала: библиотека версионированных шаблонов + разметка цен на уровне события |

## Диаграммы

Схемы встроены непосредственно в документы (единый источник, без дублей): D2 -> SVG.

| Диаграмма | Формат | Где |
|---|---|---|
| C4 Level 1: System Context | D2 -> SVG | [diagrams/c4-context.d2](diagrams/c4-context.d2), встроен в [docs/02-architecture.md](docs/02-architecture.md) |
| C4 Level 2: Containers | D2 -> SVG | [diagrams/c4-container.d2](diagrams/c4-container.d2), встроен в [docs/02-architecture.md](docs/02-architecture.md) |
| Взаимодействие сервисов (подписи на связях) | D2 -> SVG | [diagrams/service-interactions.d2](diagrams/service-interactions.d2), встроена в [docs/05-api-contracts.md](docs/05-api-contracts.md) |

## Краткое резюме решения

TicketHub — платформа продажи билетов, рассчитанная на ~4,25 млн активных покупателей в год (~17 млн билетов) и флеш-крауд до 40 тыс. одновременных пользователей в торговом зале (и до ~400 тыс. в waiting room) на старте продаж популярного концерта. Пик через API Gateway ~34 000 RPS (read ~10 000 + поллинг очереди ~20 000 + попытки холда ~4 000), peak заказов ~40 RPS.

Ключевые решения:

- **Микросервисы + Kafka**: read-path (каталог, поиск, схема зала) масштабируется независимо от write-path (бронирование, оплата) — критично при флеш-крауде.
- **Резерв мест в Redis (SET NX PX + Lua, TTL 10 мин)**, финальное подтверждение в PostgreSQL → гарантия «ни одного оверселла» без блокировок БД в пике.
- **PostgreSQL, один кластер** (шардирование не требуется — запаса хватает, 04.3.1; ключи `event_id`/`user_id` зарезервированы на будущее), синхронная репликация для платежей (RPO=0).
- **Асинхронная оплата через Saga/Outbox** — переживаем деградацию платёжного провайдера без потери заказов.
- **Виртуальная очередь (waiting room)** на старте продаж защищает booking path.
- Доступность платформы 99,95% (единый SLA), RTO ≤ 15 мин, RPO = 0 для платежей.
