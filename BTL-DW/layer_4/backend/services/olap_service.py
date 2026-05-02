from typing import Dict, Any, List, Optional
import logging
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
        next_level: Optional[str] = None,
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
        FROM {normalize_cube(cube)}
        """
        logging.getLogger(__name__).debug("MDX (children): %s", mdx)
        print("MDX (children):", mdx)

        if where:
            mdx += f" WHERE ({where})"

        try:
            result = self.client.execute_mdx(mdx)
        except Exception as e:
            logging.getLogger(__name__).exception("SSAS execute error for MDX:\n%s", mdx)
            raise RuntimeError(f"SSAS execute error: {e}\nMDX:\n{mdx}")

        rows = result.get('rows', [])
        if (not rows or len(rows) == 0) and next_level:
            alt_mdx = (
                f"SELECT {{ {measure} }} ON COLUMNS, "
                f"NON EMPTY {{ DESCENDANTS({parent_member}, {next_level}) }} DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION ON ROWS "
                f"FROM {normalize_cube(cube)}"
            )
            if where:
                alt_mdx += f" WHERE ({where})"

            logging.getLogger(__name__).debug("MDX (children fallback - descendants): %s", alt_mdx)
            print("MDX (children fallback - descendants):", alt_mdx)
            try:
                alt_result = self.client.execute_mdx(alt_mdx)
            except Exception as e:
                logging.getLogger(__name__).exception("SSAS execute error (fallback - descendants) for MDX:\n%s", alt_mdx)
                raise RuntimeError(f"SSAS execute error (fallback - descendants): {e}\nMDX:\n{alt_mdx}")
            alt_rows = alt_result.get('rows', [])
            if alt_rows and len(alt_rows) > 0:
                return alt_result

        return result

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
        FROM {normalize_cube(cube)}
        """
        logging.getLogger(__name__).debug("MDX (descendants): %s", mdx)
        print("MDX (descendants):", mdx)

        if where:
            mdx += f" WHERE ({where})"

        try:
            return self.client.execute_mdx(mdx)
        except Exception as e:
            logging.getLogger(__name__).exception("SSAS execute error for MDX:\n%s", mdx)
            raise RuntimeError(f"SSAS execute error: {e}\nMDX:\n{mdx}")

    def resolve_member_unique_name(self, cube: str, level_unique_name: str, member_label: str) -> str:
        """
        Resolve a member caption or short label to a member unique name by querying all members
        of the provided level and matching on MEMBER_CAPTION or key suffix.

        Uses a direct MDX Members query instead of $SYSTEM.MDSCHEMA_MEMBERS WHERE filtering,
        because ADOMD.NET does not support the LEVEL_UNIQUE_NAME filter in that schema rowset.
        """
        if not level_unique_name:
            raise ValueError("parent level is required to resolve a member caption")

        target = str(member_label).strip()
        if not target:
            raise ValueError("member label is required to resolve a member caption")

        if target.startswith("["):
            return target

        def normalized(value: Any) -> str:
            return str(value).strip() if value is not None else ""

        def matches_numeric_caption(value: str) -> bool:
            if not target.isdigit():
                return False
            if not value:
                return False
            compact = value.replace(" ", "")
            return target == compact or compact.endswith(target) or compact.endswith(f"{target}")

        def search_rows(rows: List[Dict[str, Any]]) -> Optional[str]:
            # First pass: exact match on caption, name, or key
            for r in rows:
                caption = normalized(r.get('MEMBER_CAPTION') or r.get('Caption'))
                name = normalized(r.get('MEMBER_NAME') or r.get('Name'))
                unique = normalized(r.get('MEMBER_UNIQUE_NAME') or r.get('UniqueName'))
                member_key = normalized(r.get('MEMBER_KEY') or r.get('Member_Key'))
                if target in {caption, name, member_key}:
                    return unique

            # Second pass: suffix and numeric match
            for r in rows:
                caption = normalized(r.get('MEMBER_CAPTION') or r.get('Caption'))
                name = normalized(r.get('MEMBER_NAME') or r.get('Name'))
                unique = normalized(r.get('MEMBER_UNIQUE_NAME') or r.get('UniqueName'))
                member_key = normalized(r.get('MEMBER_KEY') or r.get('Member_Key'))
                if unique.endswith(f"&[{target}]") or unique.endswith(f"[{target}]"):
                    return unique
                if matches_numeric_caption(caption) or matches_numeric_caption(name) or member_key == target:
                    return unique

            return None

        # Primary query: use MDX Members set to get all members at this level.
        # This avoids the ADOMD.NET bug where $SYSTEM.MDSCHEMA_MEMBERS does not
        # support WHERE filtering on LEVEL_UNIQUE_NAME.
        mdx = (
            f"SELECT {{ {level_unique_name}.Members }} "
            f"DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION, MEMBER_NAME, MEMBER_KEY ON ROWS "
            f"FROM {normalize_cube(cube)}"
        )
        logging.getLogger(__name__).debug("MDX (resolve member): %s", mdx)

        try:
            result = self.client.execute_mdx(mdx)
            rows = self._rows_as_dicts(result)
            match = search_rows(rows)
            if match:
                return match
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Primary member resolve query failed, will try fallback. Error: %s", e
            )

        # Fallback: query the schema rowset without the WHERE filter, then filter in Python.
        # Some SSAS versions do support this without the LEVEL_UNIQUE_NAME predicate.
        try:
            schema_mdx = f"SELECT * FROM $SYSTEM.MDSCHEMA_MEMBERS WHERE CUBE_NAME = '{cube}'"
            result = self.client.execute_mdx(schema_mdx)
            rows = self._rows_as_dicts(result)
            # Filter to the target level in Python
            rows = [
                r for r in rows
                if normalized(r.get('LEVEL_UNIQUE_NAME') or r.get('LevelUniqueName')) == level_unique_name
            ]
            match = search_rows(rows)
            if match:
                return match
        except Exception as e:
            logging.getLogger(__name__).warning(
                "Fallback schema rowset query also failed. Error: %s", e
            )

        raise ValueError(f"Could not resolve member '{member_label}' in level {level_unique_name}")

    # =========================
    # Helper for building members
    # =========================
    def build_time_member(self, year: int) -> str:
        """
        Helper: build member từ value (cho frontend)
        """
        return f"[0_ThoiGian].[Nam].[{year}]"