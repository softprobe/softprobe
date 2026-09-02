<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# Engineering Principles

These requirements always apply to design, implementation, refactoring, and review:

1. **DRY first.** Reuse production and test code. Keep behavior in one shared
   implementation and isolate only the genuinely backend-specific primitives.
   Do not copy lifecycle orchestration, validation, fixtures, scenarios, or
   assertions between PostgreSQL, SQLite, or other adapters.
2. **TDD and complete coverage.** Write a failing test before implementation.
   Every change requires proportionate unit, integration, and end-to-end
   coverage. Backend parity must be tested by running one shared contract suite
   against every backend, not by copying tests.
3. **Senior architect review loop.** For design and code changes, launch a
   senior-architect subagent that reads the relevant code, tests, documentation,
   and project goals. The review must be substantive, not a rubber stamp.
   Resolve every blocking finding and repeat review until green.
4. **Name by module and domain, not by plan.** Source files, modules, packages,
   tests, and identifiers must use code-module and business-domain phrases only.
   Never encode planning/roadmap vocabulary in the tree (`phase0`/`phase1`,
   leftovers, milestones, epics, sprints, “option A/B”). Specs and plans may use
   those words; implementation code must not.

Prefer shared abstractions that remove duplication over parallel implementations.
Especially enforce rules 1, 2, and 4.
