# Task: Implement the Flash-Sale Application From the Existing Architecture

Read `FLASH_SALE_ARCHITECTURE.md` in the project root before making any changes.

This file contains my existing architecture plan. Treat it as the starting specification and build the actual application from it. Do not restart the architecture exercise, rewrite the entire plan, or return another architecture-only response.

## 1. First, inspect and validate the plan

Review the existing architecture and identify only the unresolved decisions, implementation gaps, contradictions, and technical risks that could prevent a correct, runnable application.

Pay particular attention to:

- PostgreSQL as the authoritative inventory source.
- Durable checkout acceptance through the admission ledger and transactional outbox.
- Redis provisional reservations, token ownership, expiry, recovery, and reconciliation.
- Kafka message delivery, retries, duplicate processing, and consumer recovery.
- Atomic multi-unit inventory allocation and release.
- Payment callbacks racing with reservation expiry.
- Database transaction boundaries and order-state transitions.
- Bounded worker concurrency, overload behavior, and queue capacity.

Do not silently replace the approved design with a different architecture. Where a decision is unresolved, choose the simplest safe implementation consistent with the existing plan and document the decision.

If a design detail would violate inventory correctness, fix the underlying issue before implementing it and explain the correction.

## 2. Implement the complete application

Create actual source files in the current workspace. Implement the frontend, backend, database, infrastructure, and tests described in the existing plan.

The application must include:

- Product display, quantity selection, checkout, and queued, reserved, sold-out, failed, and confirmed states.
- Validated APIs, idempotency keys, durable admission, and order-status tracking.
- PostgreSQL schema, migrations, transactions, constraints, and indexes.
- Redis atomic operations where justified by the approved design.
- Kafka-based asynchronous processing and recoverable workers.
- Simulated payment processing, reservation expiry, retries, compensation, and reconciliation.
- Logging, configuration, health checks, and Docker Compose.
- Essential automated tests and a configurable load-testing script.

Support multi-unit purchases. Do not impose a one-unit-per-customer restriction.

## 3. Preserve correctness and recovery guarantees

The application must never allocate or confirm more than 500 units for this sale.

Implement inventory allocation using an atomic PostgreSQL conditional update and appropriate transactions. Make reservation creation, consumption, expiry, and release idempotent.

Do not treat Redis atomicity or Kafka delivery guarantees as substitutes for database consistency. Do not report checkout acceptance until the documented durability conditions have been satisfied.

Handle retries, duplicate messages, uncertain HTTP outcomes, worker crashes, and delayed payment callbacks safely.

If a required dependency fails, fail safely, preserve recoverable accepted requests, and resume processing when possible. Document the limits of these guarantees.

## 4. Work directly in the project

Use the available filesystem and terminal to create the application. Do not merely print source files in the response.

Implement the project in manageable stages, keeping the architecture document and source code consistent. Avoid unnecessary dependencies and do not introduce infrastructure without a clear reason.

After each major stage, check that the relevant modules integrate correctly.

## 5. Run and verify

When the implementation is complete:

1. Start the application using the documented local setup.
2. Run database migrations.
3. Run all essential automated tests.
4. Test concurrent inventory allocation, multi-unit purchases, duplicate requests, reservation expiry, payment outcomes, and end-to-end checkout.
5. Fix identified defects and rerun affected tests.
6. Inspect the final implementation against every mandatory requirement in `FLASH_SALE_ARCHITECTURE.md` and the original flash-sale requirements.

Provide a configurable load-testing script for the 100,000-request scenario. Do not claim that the system can sustain this workload unless the relevant load test has actually demonstrated it.

Clearly distinguish tests that passed, tests that failed, and tests that could not be executed.

## 6. Final deliverables

Produce:

- A complete runnable project with its actual source files.
- Database migrations and infrastructure configuration.
- Installation, environment setup, startup, and testing instructions.
- The project folder structure.
- An end-to-end checkout lifecycle walkthrough.
- A failure-analysis table.
- A requirement-validation checklist.
- Actual test results.
- Assumptions, known limitations, unresolved risks, and production-readiness requirements.

Begin by inspecting `FLASH_SALE_ARCHITECTURE.md` and the current workspace. Then proceed with implementation. Do not stop after the initial review or an implementation plan.

### Code Quality and Presentation

- Write clean, professional code that follows the conventions of the chosen languages and frameworks.
- Avoid unnecessary comments, redundant explanations, generic AI-style wording, and verbose documentation that merely restates what the code does.
- Do not include phrases such as "generated by AI," "AI-generated code," "as an AI," or similar statements in source files, UI text, README files, or documentation unless explicitly required.
- Write comments only when they explain non-obvious logic, important design decisions, security considerations, or complex concurrency handling.
- Use meaningful variable names, consistent formatting, and realistic error messages.
- Ensure the UI, README, commit-ready source files, and documentation are coherent and project-specific rather than generic templates.
- Do not remove necessary security warnings, technical explanations, attribution, license notices, or disclosures required by the project or its applicable rules.