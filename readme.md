# ДЗ-2
## 1. Используемый сервисные аккаунты: 
- vvot11-service-account
## 2. Ресурсы облака
### Бакеты
- test-bucket-isl-itis (использовался один, потому что превышен лимит на создание бакетов)
### БД
- vvot11-db-photo-face
### Очереди
- vvot11-tasks
### Триггеры
- vvot11-photo-trigger
- vvot01-task-trigger
### Функции
- vvot11-boot
- vvot11-face-detection
### Контейнер
- vvot11-face-cut
### API Gateway
- itis-2022-2023-vvot11-api

## Алиас бота: 
@yandex_cloud_task_bot

## Команды бота:
/start
/getface
/find {name}