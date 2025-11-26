import time
from typing import List

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.queue.outbox import fetch_pending_events, mark_failed, mark_processed
from src.adapters.supabase.db import PostgresPool
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.pipelines.meal_plan_pipeline import MealPlanPipeline
from src.pipelines.shopping_list_pipeline import ShoppingListPipeline
from src.utils.logging import configure_logging


TABLES = [
    "meal_plans",
    "meal_plan_items",
    "shopping_lists",
    "shopping_list_items",
]

AGG_TYPES = ["meal_plan", "shopping_list"]


def process_batch(meal_pipeline, shopping_pipeline, events: List[OutboxEvent], pg_pool: PostgresPool, log):
    for event in events:
        try:
            if event.aggregate_type == "meal_plan":
                meal_pipeline.handle_event(event)
            elif event.aggregate_type == "shopping_list":
                shopping_pipeline.handle_event(event)
            else:
                log.warning("Unhandled aggregate type", extra={"aggregate_type": event.aggregate_type, "event_id": event.id})
                continue
            with pg_pool.connection() as conn:
                mark_processed(conn, event.id)
        except Exception as exc:  # noqa: BLE001
            log.exception("Failed processing event", extra={"event_id": event.id, "aggregate_id": event.aggregate_id})
            with pg_pool.connection() as conn:
                mark_failed(conn, event.id, str(exc))


def main():
    settings = Settings()
    log = configure_logging("meal_planning_shopping_worker")
    log.info("Starting meal-planning-shopping worker", extra={"pipeline": settings.pipeline_name})

    pg_pool = PostgresPool(settings.supabase_dsn)
    neo4j = Neo4jClient(settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password)
    meal_pipeline = MealPlanPipeline(settings, pg_pool, neo4j)
    shopping_pipeline = ShoppingListPipeline(settings, pg_pool, neo4j)

    try:
        while True:
            with pg_pool.connection() as conn:
                conn.autocommit = False
                events = fetch_pending_events(
                    conn,
                    settings.batch_size,
                    settings.max_attempts,
                    table_names=TABLES,
                    aggregate_types=AGG_TYPES,
                )
                conn.commit()

            if not events:
                time.sleep(settings.poll_interval_seconds)
                continue

            process_batch(meal_pipeline, shopping_pipeline, events, pg_pool, log)
    finally:
        neo4j.close()
        pg_pool.close()


if __name__ == "__main__":
    main()
