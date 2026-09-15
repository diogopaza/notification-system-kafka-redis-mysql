# Notification System (Kafka + Redis + MySQL) — Design

Data: 2026-09-15
Status: Aprovado para implementação

## 1. Contexto e objetivo

Projeto de prática para aprender, na prática, os conceitos de sistemas distribuídos
usados para escalar um sistema de notificações (push/email/SMS) para milhões de
usuários. Baseado no artigo ["Designing a Notification System That Can Handle 10
Million Users"](https://medium.com/@gaddamnaveen192/designing-a-notification-system-that-can-handle-10-million-users-cad6b4970b78),
de Gaddam Naveen.

Este repositório é **exclusivamente prático**: sem perguntas teóricas (isso já é
coberto em outros repositórios do currículo). Cada conceito do artigo vira código
rodando de verdade + um teste automatizado que comprova o comportamento real
(Kafka, Redis e MySQL reais via Testcontainers, não mocks de unidade).

Não há credenciais reais de Firebase/Twilio/SendGrid — os três providers externos
são simulados por mocks que reproduzem o contrato e os tipos de erro reais de cada
serviço, para servir de aprendizado sobre como cada um funciona.

## 2. Escopo

Incluído (tudo do artigo):
- API de ingestão de notificações
- Fluxo assíncrono via Kafka (fan-out desacoplado da requisição HTTP)
- Particionamento por `userId` (ordenação por usuário)
- Idempotência via Redis (`SETNX`) — proteção contra double-send
- Retry com backoff exponencial + Dead Letter Queue (DLQ), por worker
- Isolamento de prioridade (tópicos `critical` / `transactional` / `marketing`)
- Rate limiting no worker de SMS (simulando limite real da Twilio)
- Scheduling via Redis Sorted Set (`ZADD` / `ZRANGEBYSCORE`)
- Mocks de FCM, Twilio e SendGrid com contrato realista

Fora de escopo (YAGNI para um projeto de prática):
- Particionamento físico de tabelas por data / arquivamento para cold storage
- Observabilidade completa (Prometheus/Grafana) — pode virar um round futuro
- Autenticação/autorização na API
- Serviço de retry genérico (decisão: retry vive dentro de cada worker de canal,
  ver seção 5)

## 3. Arquitetura e módulos

Projeto multi-módulo Maven, um serviço Spring Boot por responsabilidade —
fiel ao espírito de microsserviços do artigo:

| Módulo | Responsabilidade |
|---|---|
| `notification-common` | Biblioteca compartilhada: record `Notification`, enums (`Channel`, `Priority`, `NotificationType`), `ProviderResult`, interface `NotificationProvider<T>` |
| `notification-api` | REST: valida request, persiste em MySQL (`PENDING`), decide prioridade, publica no tópico Kafka correto, particionando por `userId` |
| `push-worker` | Consome os 3 tópicos de prioridade, filtra `channel=PUSH`, chama `FirebasePushProvider` (mock), aplica idempotência, atualiza status |
| `email-worker` | Igual ao `push-worker`, para `channel=EMAIL` / `SendGridProvider` (mock) |
| `sms-worker` | Igual ao `push-worker`, para `channel=SMS` / `TwilioProvider` (mock), com `RateLimiter` |
| `scheduler-service` | Faz polling no Redis Sorted Set; quando o horário agendado chega, publica no tópico Kafka de prioridade correspondente |

Cada worker roda como processo Spring Boot independente, com seu próprio
`docker` build, mas todos compartilham `notification-common` via dependência
Maven (mono-repo, multi-artefato).

### Fluxo principal

```
Cliente → notification-api → MySQL (PENDING) → Kafka (notification.<prioridade>)
                                                       │
                    ┌──────────────────────────────────┼──────────────────────┐
                    ▼                                  ▼                      ▼
              push-worker                        email-worker            sms-worker
           (filtra channel=PUSH)              (filtra channel=EMAIL)  (filtra channel=SMS)
                    │                                  │                      │
             Redis: idempotência                Redis: idempotência    Redis: idempotência
                    │                                  │                      │
          FirebasePushProvider (mock)          SendGridProvider (mock)  TwilioProvider (mock)
                    │                                  │                      │
                    └──────────────► MySQL: status SENT/FAILED ◄──────────────┘
```

## 4. Tópicos Kafka e particionamento

- `notification.critical` — OTP, alertas de segurança. Consumer group com alta
  concorrência dedicada.
- `notification.transactional` — confirmações de pedido, etc.
- `notification.marketing` — campanhas em massa. Pode atrasar/lagar sem afetar
  os outros tópicos — isso é o "isolamento de prioridade".
- Chave de partição: `userId`, em todos os tópicos acima — garante que
  mensagens do mesmo usuário sejam processadas em ordem por um único consumidor
  de cada grupo.
- Por worker, tópicos próprios de retry/DLQ (seção 5): `notification.push.retry`,
  `notification.push.dlq` (e equivalentes para email/sms).

Cada worker (`push-worker`, `email-worker`, `sms-worker`) assina os três
tópicos de prioridade e **descarta** (não processa, apenas faz `ack`) mensagens
cujo `channel` não seja o seu — mantém um número pequeno de tópicos (3, não 9).

## 5. Idempotência e Retry/DLQ

**Idempotência:** antes de chamar o provider, o worker executa
`SETNX idempotencyKey "PROCESSING" EX 86400` no Redis. Se a chave já existir,
a mensagem é ignorada (já foi processada) — resolve o cenário de "at-least-once"
do Kafka em que o worker processa e falha antes de commitar o offset.

**Retry — decisão de design:** o retry fica **dentro de cada worker de canal**,
não em um serviço de retry genérico. Motivo: retry significa chamar o provider
de novo, e só o `push-worker` sabe falar com o Firebase, só o `sms-worker` sabe
falar com o Twilio, etc. Um serviço de retry central precisaria conhecer todos
os providers, o que anularia a separação por canal.

Fluxo de retry (igual em cada worker):
1. Provider mock classifica o erro: `FAILED_NON_RETRYABLE` (ex: token
   inválido) → grava `FAILED` no MySQL, não tenta de novo.
2. `FAILED_RETRYABLE` (ex: timeout) → publica no próprio tópico de retry
   (ex: `notification.push.retry`) com um contador de tentativas.
3. O mesmo worker consome seu tópico de retry, espera o tempo de backoff
   exponencial com jitter (10s, 30s, 1min, 5min), e tenta de novo.
4. Depois de 5 tentativas, publica em `notification.push.dlq` e marca `DLQ`
   no MySQL para inspeção manual.

## 6. Rate limiting (backpressure)

No `sms-worker`, um `RateLimiter` (Guava) simula o limite real de TPS da
Twilio (configurável, ex: 1000/s). Se o worker tentar processar mais rápido
que isso, `rateLimiter.acquire()` bloqueia a thread consumidora — as
mensagens ficam esperando no Kafka (que serve de buffer) até o provider
"aceitar" mais tráfego. Isso é o backpressure natural citado no artigo.

## 7. Scheduling

Notificações agendadas (`scheduledAt` no futuro) são gravadas pela
`notification-api` num Redis Sorted Set (`ZADD scheduled_notifications
<timestamp> <notificationId>`), em vez de publicadas direto no Kafka.

O `scheduler-service` faz polling periódico (`ZRANGEBYSCORE
scheduled_notifications -inf <now>`), e para cada item vencido: publica no
tópico Kafka de prioridade correspondente e remove do Sorted Set (`ZREM`).

## 8. Banco de dados (MySQL)

```sql
CREATE TABLE notifications (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    channel VARCHAR(16) NOT NULL,
    priority VARCHAR(16) NOT NULL,
    template_id VARCHAR(64),
    idempotency_key VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE notification_templates (
    id VARCHAR(64) PRIMARY KEY,
    channel VARCHAR(16) NOT NULL,
    body TEXT NOT NULL,
    language VARCHAR(8) NOT NULL
);

CREATE TABLE notification_preferences (
    user_id VARCHAR(64) NOT NULL,
    channel VARCHAR(16) NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (user_id, channel)
);
```

Sem particionamento físico por data (fora de escopo, ver seção 2).

## 9. Providers mockados

Interface comum (`notification-common`):

```java
public interface NotificationProvider<T extends Notification> {
    ProviderResult send(T notification);
}
```

- **`FirebasePushProvider` (mock, em `push-worker`)** — simula FCM: um
  "token morto" configurado retorna `FAILED_NON_RETRYABLE` (equivalente ao
  erro real `UNREGISTERED` do FCM); caso contrário, chance configurável de
  `FAILED_RETRYABLE` simulando timeout.
- **`TwilioSmsProvider` (mock, em `sms-worker`)** — simula Twilio: número em
  formato inválido → `FAILED_NON_RETRYABLE`; usa o `RateLimiter` da seção 6.
- **`SendGridEmailProvider` (mock, em `email-worker`)** — simula SendGrid:
  email mal formatado → `FAILED_NON_RETRYABLE`; chance configurável de erro
  5xx → `FAILED_RETRYABLE`.

Cada mock tem um comentário curto explicando o que o serviço real faz e qual
erro real está sendo reproduzido, para servir de material de estudo.

## 10. Estratégia de testes

Testcontainers (Kafka, Redis, MySQL reais em containers efêmeros) — um teste
por conceito, cada um com breve explicação do que está sendo comprovado:

1. **Fluxo básico** — API → MySQL (`PENDING`) → Kafka → worker → provider
   mock → MySQL (`SENT`).
2. **Idempotência** — mesma mensagem publicada 2x → provider mock chamado
   apenas 1 vez.
3. **Particionamento/ordem** — duas mensagens do mesmo `userId` → mesma
   partição, processadas em ordem.
4. **Retry + DLQ** — provider mock falha as 3 primeiras vezes → mensagem
   passa por `notification.push.retry` e só confirma `SENT` na 4ª tentativa;
   configurado para falhar sempre → cai em `notification.push.dlq`.
5. **Isolamento de prioridade** — fila grande em `notification.marketing` não
   atrasa uma mensagem em `notification.critical` (grupos de consumidor
   distintos).
6. **Rate limiting** — N mensagens SMS disparadas mais rápido que o limite
   configurado → tempo total de processamento respeita o limite.
7. **Scheduling** — notificação agendada para poucos segundos no futuro →
   só é publicada no Kafka depois desse tempo, não antes.

## 11. Infraestrutura local

`docker-compose.yml` na raiz do repositório:
- Kafka (modo KRaft, sem Zookeeper)
- Redis
- MySQL
- Kafka UI (`provectuslabs/kafka-ui`) — inspeção visual de tópicos, partições,
  consumer lag e mensagens em DLQ, para reforçar os conceitos na prática.
