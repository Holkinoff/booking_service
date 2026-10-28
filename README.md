# Сервис записи на приём с уведомлениями

## Тестирование
Для запуска тестов используется тестовый compose (файл compose.test.yaml)

Команда для запуска:
```bash
docker compose -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from test-runner
```
- `--build` — пересобирает образы перед запуском
- `--abort-on-container-exit` — останавливает все сервисы при выходе любого из них
- `--exit-code-from test-runner` — возвращает код выхода контейнера test-runner (0 — тесты прошли, 1 — ошибка)