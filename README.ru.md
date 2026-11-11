# TaskRelay

**Transactional outbox и надёжные фоновые задачи для Spring Boot на PostgreSQL. Без Kafka, RabbitMQ и новой инфраструктуры: очередь лежит в вашей же базе.**

[English version](README.md)

Вы сохранили заказ, и теперь нужно отправить письмо, завести сделку в CRM или дёрнуть соседний микросервис. TaskRelay записывает это намерение **в той же транзакции**, что и заказ. Потом фоновый воркер доставляет его с ретраями, экспоненциальной задержкой, отложенным запуском и защитой от дублей. Несколько экземпляров сервиса работают одновременно без конфликтов благодаря `SELECT … FOR UPDATE SKIP LOCKED`.

```java
@Transactional
public void placeOrder(Order order) {
    orders.save(order);
    taskQueue.enqueue(new OrderConfirmationEmail(order.getId()));
}

@Component
class OrderConfirmationEmailHandler extends TaskHandler<OrderConfirmationEmail> {

    OrderConfirmationEmailHandler() {
        super("ORDER_CONFIRMATION_EMAIL", OrderConfirmationEmail.class);
    }

    @Override
    protected void handle(OrderConfirmationEmail payload, TaskContext context) {
        mailer.send(payload.orderId(), context.idempotencyKey());
    }

    @Override
    protected int maxAttempts() {
        return 6;
    }
}
```

Если транзакция откатилась, письмо не уйдёт. Если почтовый сервер лежит, таска подождёт и повторится. Если приложение перезапустилось посередине, ничего не потеряется.

## Какую проблему решает

Каждый бэкенд рано или поздно должен сделать что-то **снаружи** после того, как что-то изменил **внутри** базы. Это проблема двойной записи (dual write), и первые попытки её решить обычно ломаются:

| Попытка | Что идёт не так |
|---|---|
| HTTP-вызов внутри `@Transactional` | Письмо ушло, а транзакция откатилась: клиент получил подтверждение несуществующего заказа. Медленный вызов к тому же держит соединение с БД и блокировки строк. |
| Вызов после коммита через `@TransactionalEventListener(AFTER_COMMIT)` или `@Async` | Коммит прошёл, вызов упал или под перезапустился, и действие **потеряно навсегда**. |
| Обёртка Spring Retry / Resilience4j | Ретраи живут в памяти: блокируют поток и пропадают при рестарте. «Три попытки» не переживут провайдера, который лежит двадцать минут. |
| Своя таблица `task` и `@Scheduled`-поллер | Работает, пока второй экземпляр не возьмёт те же строки, упавший воркер не оставит таски в `RUNNING` навсегда или код не скопируют в соседний сервис, где он начнёт расходиться. |

Решение называется **transactional outbox**: сохранить намерение в той же транзакции, что и бизнес-изменение, а доставлять его фоновым воркером. TaskRelay — это такой паттерн в виде маленького Spring Boot starter поверх обычной таблицы PostgreSQL.

Типичные запросы, по которым сюда приходят:

- *отправить письмо после коммита транзакции spring*
- *transactional outbox spring boot без kafka*
- *очередь задач на postgres skip locked java*
- *ретраи с экспоненциальной задержкой spring boot*
- *@Async теряет задачи при перезапуске*
- *как не обработать задачу дважды на нескольких инстансах*
- *дедупликация фоновых задач / идемпотентность*

## Возможности

- **Постановка в транзакции.** `taskQueue.enqueue(payload)` работает внутри текущей транзакции: JPA/Hibernate, `JdbcTemplate` или jOOQ на том же `DataSource`.
- **Типизированные обработчики.** Один наследник `TaskHandler<P>` на каждый тип таски со своим классом payload (можно record).
- **Ретраи с экспоненциальной задержкой.** Задаются в обработчике: `maxAttempts()`, `retryable(e)`, `retryDelay(attempt)`.
- **Отложенный запуск.**
  - Запуск в нужный момент: `withRunAt` / `withDelay`.
  - Обработка только «отлежавшихся» тасок: `delay()`.
- **Дедупликация.** С `withDedupeKey("payment-42")` повторная постановка ничего не делает; это гарантирует уникальный индекс.
- **Безопасно для нескольких экземпляров.** Таски захватываются через `FOR UPDATE SKIP LOCKED`. Таски упавшего воркера подбираются заново после `lock-timeout`.
- **Мягкая остановка.** Захваченные, но не начатые таски возвращаются в очередь.
- **Пропускная способность по типам.** `batch-size`, `poll-interval`, `lock-timeout` и `enabled` настраиваются отдельно для каждого типа (удобно для лимитов провайдеров).
- **Отдельный поток для медленных обработчиков.**
- **Схема настраивается сама.** Таблица создаётся, если её нет. В существующую таблицу недостающие колонки дописываются на месте, причём уже после Flyway или Liquibase.
- **Payload в Jackson или Gson**, либо свой `TaskPayloadCodec`.
- **Опциональная чистка** завершённых тасок.

## Быстрый старт

**1. Зависимость** (через JitPack):

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.yanisderbikov</groupId>
    <artifactId>taskrelay</artifactId>
    <version>0.1.0</version>
</dependency>
```

**2. Payload.** Храните в нём id, а не целые сущности:

```java
public record WelcomeEmail(UUID userId) {
}
```

**3. Обработчик:**

```java
@Component
class WelcomeEmailHandler extends TaskHandler<WelcomeEmail> {

    private final Users users;
    private final Mailer mailer;

    WelcomeEmailHandler(Users users, Mailer mailer) {
        super("WELCOME_EMAIL", WelcomeEmail.class);
        this.users = users;
        this.mailer = mailer;
    }

    @Override
    protected void handle(WelcomeEmail payload, TaskContext context) {
        User user = users.findById(payload.userId()).orElseThrow();
        mailer.send(user.email(), "Добро пожаловать!", context.idempotencyKey());
    }

    @Override
    protected int maxAttempts() {
        return 5;
    }
}
```

**4. Постановка в бизнес-транзакции:**

```java
@Transactional
public User register(SignUp request) {
    User user = users.save(new User(request.email()));
    taskQueue.enqueue(new WelcomeEmail(user.id()));
    return user;
}
```

Больше ничего не нужно. При старте TaskRelay создаст таблицу `taskrelay_task`, запустит по воркеру на каждый обработчик и будет разбирать очередь раз в 5 секунд.

## Гарантии доставки

| Настройка | Гарантия | Когда использовать |
|---|---|---|
| `maxAttempts() == 1` (по умолчанию) | **Не больше одного раза.** Ошибка окончательна. Таска, чей воркер умер посередине, уходит в `FAILED`, а не выполняется повторно. | Действия, которые нельзя ни дублировать, ни сделать идемпотентными. |
| `maxAttempts() > 1` | **Хотя бы один раз.** Ошибки повторяются, таску упавшего воркера подберут после `lock-timeout`. | Всё, что можно сделать идемпотентным: письма с ключом идемпотентности, upsert, PUT. |

`context.idempotencyKey()` возвращает `task:<uuid>`. Ключ одинаков для всех попыток одной таски, поэтому его стоит передавать во внешние API, которые поддерживают идемпотентность.

## Ретраи, задержки, дедупликация

| Метод обработчика | По умолчанию | Смысл |
|---|---|---|
| `maxAttempts()` | `1` | Всего попыток, включая первую. |
| `retryable(Exception e)` | `true` | `false` для постоянных ошибок (валидация, 4xx), чтобы сразу уйти в `FAILED`. |
| `retryDelay(int attempt)` | 1м, 2м, 4м … максимум 1ч | Пауза после неудачной попытки *n*. |
| `delay()` | `0` | Не брать таску, пока она не «отлежится» столько времени. |
| `dedicatedThread()` | `false` | Отдельный поток, чтобы медленные вызовы не тормозили остальные типы. |

```java
taskQueue.enqueue(new ReminderPush(userId), EnqueueOptions.defaults().withDelay(Duration.ofHours(2)));

Optional<UUID> id = taskQueue.enqueue(new FailedPaymentLead(paymentId),
        EnqueueOptions.defaults().withDedupeKey(paymentId.toString()));
```

## Настройки

| Свойство | По умолчанию | Описание |
|---|---|---|
| `taskrelay.table-name` | `taskrelay_task` | Таблица, можно со схемой. |
| `taskrelay.payload-format` | `jackson` | `jackson` или `gson`. |
| `taskrelay.poll-interval` | `5s` | Пауза между пачками для каждого типа. |
| `taskrelay.initial-delay` | `10s` | Задержка перед первым опросом. |
| `taskrelay.batch-size` | `50` | Сколько тасок захватывать за раз. |
| `taskrelay.lock-timeout` | `15m` | На сколько резервируется пачка; должно быть больше времени её обработки. |
| `taskrelay.threads` | `1` | Общих потоков воркеров. |
| `taskrelay.shutdown-timeout` | `30s` | Сколько ждать текущие таски при остановке. |
| `taskrelay.retention` | хранить всегда | Удалять `DONE` старше указанного срока. |
| `taskrelay.worker.enabled` | `true` | `false` оставляет `TaskQueue`, но не запускает воркеры (удобно в тестах). |
| `taskrelay.schema.auto-create` | `true` | `false`, если таблицей управляют ваши миграции. |
| `taskrelay.types[TYPE].*` | — | `batch-size`, `poll-interval`, `lock-timeout`, `enabled` для одного типа. |

Ключи типов пишите в квадратных скобках: без них Spring Boot выбрасывает `_` из ключей map. `@EnableScheduling` не нужен: у TaskRelay свои потоки.

## Тесты обработчиков

Обработчик — обычный класс. Вызывайте `handle` напрямую из теста в его пакете:

```java
handler.handle(new WelcomeEmail(userId), new TaskContext(UUID.randomUUID(), "WELCOME_EMAIL", 1, Instant.now()));
```

В `@SpringBootTest` удобно ставить `taskrelay.worker.enabled=false` и проверять, что поставилось в очередь (`taskQueue.findByType(...)`).

## Переезд со своей таблицы тасок

1. `taskrelay.table-name=task`: недостающие колонки добавятся при старте, старые строки останутся валидными.
2. Если payload писался через Gson, укажите `taskrelay.payload-format=gson`.
3. Каждый раннер превращается в `TaskHandler` с тем же строковым типом, что уже лежит в таблице (обычно `name()` старого enum).
4. Сущность, репозиторий, поллер и enum удаляются, а `taskAdder.addTask(payload)` превращается в `taskQueue.enqueue(payload)`.

## Claude Code

В репозитории есть skill для Claude Code. Он учит Claude добавлять типы тасок, выбирать политику ретраев, писать тесты и не совершать классических ошибок с outbox.

```text
/plugin marketplace add yanisderbikov/taskrelay
/plugin install taskrelay@taskrelay
```

## История

TaskRelay не придумывали на доске, его вынули из продакшена.

**Июнь 2026.** Бэкенду [anyforms](https://anyforms.ru) понадобилось отправлять письмо после оплаты. Вместо отправки прямо в вебхуке платежа письмо стало строкой в таблице `task`, а `@Scheduled`-раннер забирал строки и отправлял.

**Лето–осень 2026.** Приём оказался удачным, и типов тасок стало больше десяти:
- письма о покупке и чеки;
- сделки в amoCRM по оплатам и неудачным оплатам;
- письма о статусе доставки СДЭК;
- промокоды из попапов;
- запуск салесботов;
- отложенная проверка «ответил ли менеджер клиенту за 30 минут».

По дороге раннер научился ретраям с задержкой и отдельному потоку для медленных проверок в CRM.

**Август 2026.** Второму сервису, платформе курсов, понадобились письма «открыт новый модуль», и код тасок скопировали туда. Копия застыла в тот же день: ни ретраев, ни защиты от наложения прогонов, ни отдельного потока.

**Ноябрь 2026.** Письма выносили в отдельный микросервис, и возник вопрос: зачем вообще класть таску в таблицу, если можно дёрнуть сервис по HTTP с тремя ретраями? Ответ свёлся к выгодам outbox:
- атомарность с бизнес-транзакцией;
- никаких заблокированных потоков под блокировками БД;
- устойчивость к перезапускам;
- стабильный ключ идемпотентности.

Попутно вскрылись слабые места самописной версии:
- четыре раннера писем никогда не ретраились;
- два экземпляра могли взять одну строку;
- падение оставляло таски в `RUNNING` навсегда;
- защита от дублей искала по payload через `LIKE`.

Чинить одно и то же дважды не стали и вынесли код в эту библиотеку. Недостающее в ней есть с первого дня: захват через `SKIP LOCKED`, истечение блокировок, настоящие задержки, ключи дедупликации, строковые типы и схема, которая обновляет старые таблицы на месте. Оба сервиса переезжают на TaskRelay, а каждый следующий начинает сразу с неё.

Автор: [Yan Derbikov](https://github.com/yanisderbikov).

## Лицензия

[Apache License 2.0](LICENSE)
