from typing import Dict, Any, List, Optional
from ssas_client import SSASClient
from mdx_builder import build_simple_mdx
from mdx_builder import normalize_cube


class OLAPService:
    def __init__(self, client: SSASClient = None):
        self.client = client or SSASClient()

    # =========================
    # Helpers
    # =========================
    def _rows_as_dicts(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        columns = result.get("columns", [])
        return [dict(zip(columns, row)) for row in result.get("rows", [])]

    def _validate_member(self, member: str):
        if not member or not member.startswith("["):
            raise ValueError(
                f"Invalid MDX member: {member}. Expected format: [Dimension].[Hierarchy].[Member]"
            )

    # =========================
    # Metadata APIs
    # =========================
    def list_cubes(self) -> Dict[str, Any]:
        query = """
        SELECT CUBE_NAME, CUBE_CAPTION
        FROM $SYSTEM.MDSCHEMA_CUBES
        WHERE CUBE_SOURCE = 1
        """
        return self.client.execute_mdx(query)

    def list_measures(self, cube: str) -> List[Dict[str, Any]]:
        query = f"""
        SELECT *
        FROM $SYSTEM.MDSCHEMA_MEASURES
        WHERE CUBE_NAME = '{cube}'
        """
        result = self.client.execute_mdx(query)
        rows = self._rows_as_dicts(result)

        return [
            {
                "name": r.get("MEASURE_NAME"),
                "caption": r.get("MEASURE_CAPTION") or r.get("MEASURE_NAME"),
                "unique_name": r.get("MEASURE_UNIQUE_NAME"),
            }
            for r in rows
            if r.get("MEASURE_IS_VISIBLE") is not False
        ]

    def list_dimensions(self, cube: str) -> List[Dict[str, Any]]:
        query = f"""
        SELECT *
        FROM $SYSTEM.MDSCHEMA_DIMENSIONS
        WHERE CUBE_NAME = '{cube}'
        """
        result = self.client.execute_mdx(query)
        rows = self._rows_as_dicts(result)

        return [
            {
                "name": r.get("DIMENSION_NAME"),
                "caption": r.get("DIMENSION_CAPTION") or r.get("DIMENSION_NAME"),
                "unique_name": r.get("DIMENSION_UNIQUE_NAME"),
                "default_hierarchy": r.get("DEFAULT_HIERARCHY"),
            }
            for r in rows
            if r.get("DIMENSION_IS_VISIBLE") is not False
        ]

    def list_levels(self, cube: str, hierarchy: str) -> List[Dict[str, Any]]:
        query = f"""
        SELECT *
        FROM $SYSTEM.MDSCHEMA_LEVELS
        WHERE CUBE_NAME = '{cube}'
        """
        result = self.client.execute_mdx(query)
        rows = self._rows_as_dicts(result)

        levels = [
            {
                "name": r.get("LEVEL_NAME"),
                "unique_name": r.get("LEVEL_UNIQUE_NAME"),
                "caption": r.get("LEVEL_CAPTION") or r.get("LEVEL_NAME"),
                "number": r.get("LEVEL_NUMBER"),
            }
            for r in rows
            if r.get("HIERARCHY_UNIQUE_NAME") == hierarchy
        ]

        return sorted(levels, key=lambda x: x.get("number") or 0)

    # =========================
    # Query APIs
    # =========================
    def query(
        self,
        cube: str,
        measure: str,
        hierarchy: str,
        level: Optional[str] = None,
        where: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        mdx = build_simple_mdx(cube, measure, hierarchy, level, where, limit)
        return self.client.execute_mdx(mdx)

    # =========================
    # Drilldown APIs
    # =========================
    def drilldown_children(
        self,
        cube: str,
        measure: str,
        parent_member: str,
        where: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Drilldown using .Children
        """
        self._validate_member(parent_member)

        mdx = f"""
        SELECT
            {{ {measure} }} ON COLUMNS,
            NON EMPTY
            {{ {parent_member}.Children }} ON ROWS
        FROM [{cube}]
        """

        if where:
            mdx += f" WHERE ({where})"

        return self.client.execute_mdx(mdx)

    def drilldown_descendants(
        self,
        cube: str,
        measure: str,
        parent_member: str,
        level: str,
        where: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Drilldown using DESCENDANTS (flexible hơn)
        """
        self._validate_member(parent_member)

        mdx = f"""
        SELECT
            {{ {measure} }} ON COLUMNS,
            NON EMPTY
            {{ DESCENDANTS({parent_member}, {level}) }} ON ROWS
        FROM [{cube}]
        """

        if where:
            mdx += f" WHERE ({where})"

        return self.client.execute_mdx(mdx)

    def resolve_member_unique_name(self, cube: str, level_unique_name: str, member_label: str) -> str:
        """
        Resolve a member caption or short label to a member unique name by querying all members
        of the provided level and matching on MEMBER_CAPTION or key suffix.
        """
        if not level_unique_name:
            raise ValueError("parent level is required to resolve a member caption")

        mdx = (
            f"SELECT {{ {level_unique_name}.Members }} DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION ON ROWS "
            f"FROM {normalize_cube(cube)}"
        )

        result = self.client.execute_mdx(mdx)
        rows = self._rows_as_dicts(result)

        target = str(member_label).strip()

        # First try exact caption match
        for r in rows:
            caption = r.get('MEMBER_CAPTION') or r.get('Caption') or ''
            unique = r.get('MEMBER_UNIQUE_NAME') or r.get('UniqueName') or ''
            if caption == target:
                return unique

        # Then try matching by numeric/key suffix (e.g., &[2025])
        for r in rows:
            unique = r.get('MEMBER_UNIQUE_NAME') or ''
            if unique.endswith(f"&[{target}]") or unique.endswith(f"[{target}]"):
                return unique

        raise ValueError(f"Could not resolve member '{member_label}' in level {level_unique_name}")

    # =========================
    # Helper for building members
    # =========================
    def build_time_member(self, year: int) -> str:
        """
        Helper: build member từ value (cho frontend)
        """
        return f"[0_ThoiGian].[Nam].[{year}]"