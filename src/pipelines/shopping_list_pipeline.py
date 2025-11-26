from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class ShoppingListPipeline:
    """Upsert shopping lists with items into Neo4j."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("shopping_list_pipeline")

    # ===================== DATA LOADERS =====================
    def load_shopping_list(self, conn, list_id: str) -> Optional[Dict]:
        sql = """
        SELECT sl.*, h.household_name, h.household_type,
               v.id AS vendor_id, v.name AS vendor_name
        FROM shopping_lists sl
        LEFT JOIN households h ON h.id = sl.household_id
        LEFT JOIN vendors v ON v.id = sl.vendor_id
        WHERE sl.id = %s;
        """
        return pg.fetch_one(conn, sql, (list_id,))

    def load_items(self, conn, list_id: str) -> List[Dict]:
        sql = """
        SELECT sli.*,
               p.id AS product_id, p.name AS product_name,
               i.id AS ingredient_id, i.name AS ingredient_name
        FROM shopping_list_items sli
        LEFT JOIN products p ON p.id = sli.product_id
        LEFT JOIN ingredients i ON i.id = sli.ingredient_id
        WHERE sli.shopping_list_id = %s;
        """
        return pg.fetch_all(conn, sql, (list_id,))

    def load_meal_plan_id(self, conn, list_id: str) -> Optional[str]:
        sql = "SELECT meal_plan_id FROM shopping_lists WHERE id = %s;"
        row = pg.fetch_one(conn, sql, (list_id,))
        if row and row.get("meal_plan_id"):
            return row["meal_plan_id"]
        return None

    # ===================== CYPHER =====================
    def _upsert_cypher(self) -> str:
        return """
        MERGE (h:Household {id: $shopping_list.household_id})
        SET h.household_name = $shopping_list.household_name,
            h.household_type = $shopping_list.household_type

        MERGE (sl:ShoppingList {id: $shopping_list.id})
        SET sl.list_name = $shopping_list.list_name,
            sl.total_estimated_cost = $shopping_list.total_estimated_cost,
            sl.status = $shopping_list.status,
            sl.created_at = datetime($shopping_list.created_at),
            sl.updated_at = datetime($shopping_list.updated_at)

        MERGE (h)-[:HAS_SHOPPING_LIST]->(sl)

        // Vendor (optional)
        WITH sl
        FOREACH (_ IN CASE WHEN $vendor.id IS NULL THEN [] ELSE [1] END |
          MERGE (v:Vendor {id: $vendor.id})
          SET v.name = $vendor.name
          MERGE (sl)-[:TARGETS_VENDOR]->(v)
        )

        // Clear old items
        WITH sl
        OPTIONAL MATCH (sl)-[old:HAS_ITEM]->(oldItem:ShoppingListItem)
        DETACH DELETE oldItem;

        WITH sl, $items AS items
        UNWIND items AS it
          MERGE (sli:ShoppingListItem {id: it.id})
          SET sli.item_name = it.item_name,
              sli.quantity = it.quantity,
              sli.unit = it.unit,
              sli.category = it.category,
              sli.estimated_price = it.estimated_price,
              sli.actual_price = it.actual_price,
              sli.is_purchased = it.is_purchased,
              sli.notes = it.notes,
              sli.created_at = datetime(it.created_at)
          MERGE (sl)-[:HAS_ITEM]->(sli)
          FOREACH (_ IN CASE WHEN it.product_id IS NULL THEN [] ELSE [1] END |
            MERGE (p:Product {id: it.product_id})
            SET p.name = coalesce(it.product_name, p.name)
            MERGE (sli)-[:USES_PRODUCT]->(p)
          )
          FOREACH (_ IN CASE WHEN it.ingredient_id IS NULL THEN [] ELSE [1] END |
            MERGE (ing:Ingredient {id: it.ingredient_id})
            SET ing.name = coalesce(it.ingredient_name, ing.name)
            MERGE (sli)-[:USES_INGREDIENT]->(ing)
          )
          FOREACH (_ IN CASE WHEN it.substituted_product_id IS NULL THEN [] ELSE [1] END |
            MERGE (sp:Product {id: it.substituted_product_id})
            MERGE (sli)-[:SUBSTITUTE_PRODUCT]->(sp)
          );

        // Link to meal plan if provided
        WITH sl
        FOREACH (_ IN CASE WHEN $meal_plan_id IS NULL THEN [] ELSE [1] END |
          MATCH (mp:MealPlan {id: $meal_plan_id})
          MERGE (mp)-[:GENERATES_LIST]->(sl)
        );
        """

    def _delete_cypher(self) -> str:
        return "MATCH (sl:ShoppingList {id: $id}) DETACH DELETE sl;"

    # ===================== OPERATIONS =====================
    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            shopping_list = self.load_shopping_list(conn, event.aggregate_id)

        if shopping_list is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting shopping list", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning("Shopping list missing in Supabase; skipping", extra={"id": event.aggregate_id, "op": event.op})
            return

        with self.pg_pool.connection() as conn:
            items = self.load_items(conn, event.aggregate_id)
            meal_plan_id = self.load_meal_plan_id(conn, event.aggregate_id)

        params = {
            "shopping_list": shopping_list,
            "vendor": {
                "id": shopping_list.get("vendor_id"),
                "name": shopping_list.get("vendor_name"),
            },
            "items": items,
            "meal_plan_id": meal_plan_id,
        }
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info("Upserted shopping list", extra={"id": event.aggregate_id, "items": len(items)})
