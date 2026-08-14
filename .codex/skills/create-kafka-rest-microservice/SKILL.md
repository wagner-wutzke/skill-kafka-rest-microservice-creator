---
name: create-kafka-rest-microservice
description: Create or complete a Java Maven Spring Boot microservice with REST CRUD endpoints, JPA/H2 persistence, an Avro-backed Kafka producer and consumer, externalized configuration, DLQ handling, and isolated tests. Use for event-driven REST services whose domain contract is supplied by an Avro schema in this skill's references directory.
---

# Create a Kafka + REST microservice

Create a production-shaped Java Maven service in the current workspace. Inspect before editing, preserve unrelated
changes, and follow repository conventions.

## 1. Resolve inputs before coding

Read, in order:

1. Repository-local instructions (`AGENTS.md`, `.codex/AGENTS.md`, and equivalent files).
2. `.codex/config.toml`, when present; use it for project defaults.
3. `pom.xml`, Java/source layout, tests, resources, and existing configuration.

Request values override repository configuration. If a required value remains ambiguous, state the chosen value before
editing. Use these variables:

- `project_name`: Maven artifact and service name.
- `package_name`: valid Java root package.
- `domain_object`: singular Java entity name, such as `Product`; derive `domain_object_lower` for paths.
- `app_port`: positive HTTP port; default `9000`.
- `kafka_bootstrap_servers`: broker list; default `localhost:9092`.
- `kafka_pub_topic`: publication topic; default `${domain_object_lower}s-topic`.

Resolve `references/${domain_object_lower}-model.avsc` from this skill directory. If it is missing or invalid JSON/Avro,
stop and report the problem; never invent a schema. Validate its record name, namespace, fields, logical types,
nullability, defaults, and decimal precision/scale before mapping it. Preserve the contract exactly. Copy it to the
build's configured Avro source directory only when the repository's build requires that, and compare the copies.

Record whether the project is new or existing. Never delete source, tests, generated files, local database files, or
unrelated untracked files to make generation easier. On reruns, reconcile only the requested layers.

## 2. Build and project structure

Use Maven and the repository's Java/Spring Boot versions. For a new service, use this layered structure:

```text
${package_name}
├── ${domain_object}Application.java
├── config/                 # Kafka, persistence, auditing, and properties
├── controller/             # REST endpoints and HTTP error mapping
├── domain/                 # JPA entities only
├── dto/                    # validated request/response types
├── mapper/                 # entity/DTO/Avro conversions
├── messaging/              # producer, consumer, and event types
├── repository/
└── service/
```

Preserve compatible dependencies and annotation-processor configuration already present. For a new project, include
Spring Web, Data JPA, Validation, H2, Spring Kafka, Avro, MapStruct, Lombok when appropriate, and test dependencies.
Configure Avro generation during `generate-sources` or the existing equivalent and MapStruct annotation processing.
Do not add versions or dependencies without checking the current `pom.xml`; use stable versions compatible with the
project rather than blindly selecting “latest”.

## 3. Domain, DTOs, and persistence

- Create `${domain_object}` with `UUID id`, `createdAt`, and `modifiedAt` when those fields fit the contract.
- Use `@Id`/`@GeneratedValue` and Spring Data auditing (`@CreatedDate`, `@LastModifiedDate`) when timestamps are
  application-managed.
- Map every schema field deliberately, including UUID, decimal, logical, nullable, and collection types. Use explicit
  column precision/scale where needed.
- Add `${domain_object}Repository extends JpaRepository<${domain_object}, UUID>`.
- Keep entities and generated Avro records out of REST APIs.
- Use separate validated request and response DTOs; do not accept client-controlled IDs or audit timestamps unless the
  contract requires them.
- Use one mapper component for entity/DTO and entity/Avro conversion. Instantiate generated Avro records with
  `.newBuilder()...build()`; construct DTOs/entities using the project's established style.
- Prefer constructor injection, Lombok for `@NoArgsConstructor`, `@RequiredArgsConstructor`, and readable Java formatting.

## 4. REST API

Create `${package_name}.controller.${domain_object}Controller` at `/api/v1/${domain_object_lower}s` with:

- `GET /{id}` lookup;
- `GET /` pagination using `page` default `0` and `pageSize` default `20`, sorted by `createdAt` descending;
- `POST` returning `201 Created` and a `Location` header;
- `PUT /{id}` returning the updated representation;
- `DELETE /{id}` returning `204 No Content`.

Use `@Valid`, explicit constraints, consistent 400 responses, and `ProblemDetail` or the repository's established
equivalent for 404 and other errors. Validate page parameters and test valid, invalid, missing, and update paths.

## 5. Kafka and configuration

- Keep broker addresses, topics, consumer groups, Schema Registry URL, database settings, and credentials in
  `application.yml` or configuration properties. Never hardcode them in annotations or business logic.
- Configure a producer for the generated Avro event, using the entity UUID as the stable key and
  `enable.idempotence=true`, `acks=all`, and bounded retries (for example `3`). Do not combine idempotence with
  `acks=1`.
- Do not publish before the database commit. Prefer an application event handled by
  `@TransactionalEventListener(phase = AFTER_COMMIT)` or use an explicitly documented transactional outbox. Explain
  the reliability tradeoff; an asynchronous send failure is not a successful business operation.
- Configure an explicit externalized consumer group, safe/idempotent processing, and a bounded `DefaultErrorHandler`
  with `DeadLetterPublishingRecoverer` or the version-compatible equivalent. Document the DLQ naming convention.
- Add producer and consumer unit tests that do not require a live broker. Use embedded Kafka/Testcontainers only for an
  explicitly requested integration test.
- Ensure the event payload and topic follow the repository's AsyncAPI specification as well as the Avro schema; inspect
  both when they disagree and report the conflict before changing routing.

Configure local H2 as file-based (for example `jdbc:h2:file:./data/${domain_object_lower}s-db`), expose `/h2-console`
only as appropriate for local development, disable SQL logging by default, and set `server.port` with an environment
override. Keep secrets out of source control.

## 6. Code formatting requirements

- Follow the repository formatter when one is configured; otherwise use conventional Java formatting.
- Use four spaces for indentation, no tabs, and braces on the same line as declarations.
- Keep lines at or below 120 characters where practical; break long annotations, generic types, and method calls.
- Use one import per line, remove unused imports, and keep imports organized by the project formatter.
- Add concise Javadoc only when public API behavior is non-obvious or required by the repository's quality checks.
- Keep Java readable: avoid one-line classes, methods, or multiple unrelated statements separated by semicolons.

## 7. Code style and safety

- Use UTF-8, four-space indentation, descriptive names, and the existing project formatter.
- Prefer immutable DTOs, constructor injection, early returns, constants over magic values, and explicit types over
  `var`.
- Make method parameters `final` only when that matches the repository's established style; do not introduce noisy style
  changes into unrelated files.
- Use Lombok logging only when Lombok is already enabled or explicitly selected.
- Log useful non-sensitive context with parameterized messages; never log credentials, tokens, or full sensitive
  payloads.

## 8. Tests and acceptance checks

Add focused JUnit 5 tests for mapper, validation, service, controller, repository, producer, consumer, and error paths.
Prefer `@WebMvcTest` for MVC behavior, `@DataJpaTest` for repository behavior, and mocks for Kafka. For Spring Boot 4,
use the dedicated MVC test starter and current Spring test bean-override APIs when required by the project.

Configure JaCoCo to enforce at least 95% line coverage during `verify`. Exclude only generated sources and a trivial
bootstrap class when justified; never exclude business code to make the threshold pass.

Before reporting completion:

1. Validate the reference schema and generated sources.
2. Run focused tests, then `mvn verify` or the repository-prescribed Maven command.
3. Inspect `target/site/jacoco/jacoco.csv` or the equivalent report and report actual coverage.
4. Review `git diff` and `git status`; mention pre-existing or unrelated changes.

Do not report success if the schema is missing, tests were skipped, or verification passed only by ignoring failures. If
dependencies cannot be downloaded, report the exact command and environmental blocker.
