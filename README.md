# Pipeline: meal-planning-shopping

Scope: Ingest domain-specific changes from Supabase Gold v3 into Neo4j v3 using Python workers. This repo stub is self-contained (no shared code) per requirement.

Supabase source tables: meal_plans, meal_plan_items, shopping_lists, shopping_list_items
Neo4j labels touched: MealPlan, MealPlanItem, ShoppingList, ShoppingListItem, Recipe, Product, Ingredient, Household, B2CCustomer, B2BCustomer, Vendor
Neo4j relationships touched: HAS_ITEM, USES_RECIPE, USES_PRODUCT, USES_INGREDIENT

Folders
- docs/: domain notes, Cypher patterns, event routing
- src/: config, adapters (supabase, neo4j, queue), domain models/services, pipelines (aggregate upserts), workers (runners), utils
- tests/: placeholder for unit/integration tests
- ops/: ops templates (docker/env/sample cron jobs)

Status: skeleton only; no pipeline logic yet.
