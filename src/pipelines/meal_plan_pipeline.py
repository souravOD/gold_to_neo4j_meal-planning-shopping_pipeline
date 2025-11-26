from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class MealPlanPipeline:
    """Upsert meal plans with items into Neo4j."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("meal_plan_pipeline")

    # ===================== DATA LOADERS =====================
    def load_meal_plan(self, conn, meal_plan_id: str) -> Optional[Dict]:
        sql = """
        SELECT mp.*, h.household_name, h.household_type
        FROM meal_plans mp
        LEFT JOIN households h ON h.id = mp.household_id
        WHERE mp.id = %s;
        """
        return pg.fetch_one(conn, sql, (meal_plan_id,))

    def load_items(self, conn, meal_plan_id: str) -> List[Dict]:
        sql = """
        SELECT mpi.*, r.id AS recipe_id, r.title AS recipe_name
        FROM meal_plan_items mpi
        JOIN recipes r ON r.id = mpi.recipe_id
        WHERE mpi.meal_plan_id = %s
        ORDER BY mpi.meal_date, mpi.meal_type;
        """
        return pg.fetch_all(conn, sql, (meal_plan_id,))

    # ===================== CYPHER =====================
    def _upsert_cypher(self) -> str:
        return """
        MERGE (h:Household {id: $meal_plan.household_id})
        SET h.household_name = $meal_plan.household_name,
            h.household_type = $meal_plan.household_type

        MERGE (mp:MealPlan {id: $meal_plan.id})
        SET mp.list_name = $meal_plan.plan_name,
            mp.start_date = $meal_plan.start_date,
            mp.end_date = $meal_plan.end_date,
            mp.status = $meal_plan.status,
            mp.frequency = $meal_plan.frequency,
            mp.created_at = datetime($meal_plan.created_at),
            mp.updated_at = datetime($meal_plan.updated_at)

        MERGE (h)-[:HAS_MEAL_PLAN]->(mp)

        // Clear old items
        WITH mp
        OPTIONAL MATCH (mp)-[oldRel:HAS_ITEM]->(oldItem:MealPlanItem)
        DETACH DELETE oldItem;

        WITH mp, $items AS items
        UNWIND items AS it
          MERGE (mpi:MealPlanItem {id: it.id})
          SET mpi.meal_date = it.meal_date,
              mpi.meal_type = it.meal_type,
              mpi.servings = it.servings,
              mpi.for_member_ids = it.for_member_ids,
              mpi.estimated_cost = it.estimated_cost,
              mpi.calories_per_serving = it.calories_per_serving,
              mpi.status = it.status,
              mpi.rating = it.rating,
              mpi.notes = it.notes,
              mpi.created_at = datetime(it.created_at)
          MERGE (mp)-[:HAS_ITEM]->(mpi)
          MERGE (r:Recipe {id: it.recipe_id})
          SET r.title = coalesce(it.recipe_name, r.title)
          MERGE (mpi)-[:USES_RECIPE]->(r);
        """

    def _delete_cypher(self) -> str:
        return "MATCH (mp:MealPlan {id: $id}) DETACH DELETE mp;"

    # ===================== OPERATIONS =====================
    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            meal_plan = self.load_meal_plan(conn, event.aggregate_id)

        if meal_plan is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting meal plan", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Meal plan missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            items = self.load_items(conn, event.aggregate_id)

        params = {
            "meal_plan": meal_plan,
            "items": items,
        }
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info("Upserted meal plan", extra={"id": event.aggregate_id, "items": len(items)})
