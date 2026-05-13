from fastapi import APIRouter, HTTPException
from typing import List
import logging

from models.schemas import QueryRequest, QueryResponse, CubeInfo, MetadataItem, LevelInfo
from services.olap_service import OLAPService

router = APIRouter()
service = OLAPService()


def _is_full_hierarchy_ref(value: str) -> bool:
    return bool(value and value.startswith("[") and "].[" in value)


def _normalize_hierarchy_ref(cube: str, hierarchy_or_dim: str) -> str:
    """
    If hierarchy_or_dim looks like a dimension-only reference (e.g., [1_MatHang]),
    try to resolve it to a full hierarchy reference using metadata.
    Otherwise, return as-is.
    """
    if not hierarchy_or_dim or not hierarchy_or_dim.startswith("["):
        return hierarchy_or_dim

    # Check if it has hierarchy component: [Dim].[Hier]
    first_close = hierarchy_or_dim.find("]")
    if first_close == -1 or first_close + 1 >= len(hierarchy_or_dim):
        return hierarchy_or_dim

    if hierarchy_or_dim[first_close + 1] == "." and _is_full_hierarchy_ref(hierarchy_or_dim):
        # Already a full hierarchy reference
        return hierarchy_or_dim

    # It's a dimension-only reference; try to resolve to a full hierarchy.
    try:
        dims = service.list_dimensions(cube)
        for dim in dims:
            if dim.get("unique_name") == hierarchy_or_dim:
                default_hier = dim.get("default_hierarchy")
                if _is_full_hierarchy_ref(default_hier):
                    logging.getLogger(__name__).info(
                        "Resolved dimension-only ref %s to default hierarchy %s",
                        hierarchy_or_dim,
                        default_hier,
                    )
                    return default_hier

                hierarchies = service.list_hierarchies(cube, hierarchy_or_dim)
                if hierarchies:
                    preferred = next(
                        (
                            h
                            for h in hierarchies
                            if h.get("is_default") and _is_full_hierarchy_ref(h.get("unique_name"))
                        ),
                        None,
                    )
                    resolved_hierarchy = (preferred or next((h for h in hierarchies if _is_full_hierarchy_ref(h.get("unique_name"))), hierarchies[0])).get("unique_name")
                    if resolved_hierarchy:
                        logging.getLogger(__name__).info(
                            "Resolved dimension-only ref %s to hierarchy %s",
                            hierarchy_or_dim,
                            resolved_hierarchy,
                        )
                        return resolved_hierarchy
                break
    except Exception as e:
        logging.getLogger(__name__).warning("Failed to resolve hierarchy: %s", e)

    # If we can't resolve, return as-is (let validation catch the error later)
    return hierarchy_or_dim


@router.get("/cubes", response_model=List[CubeInfo])
def list_cubes():
    result = service.list_cubes()
    columns = result.get("columns", [])
    rows = result.get("rows", [])

    if not rows:
        return []

    try:
        name_idx = columns.index("CUBE_NAME")
    except ValueError:
        name_idx = 0

    try:
        caption_idx = columns.index("CUBE_CAPTION")
    except ValueError:
        caption_idx = name_idx

    return [
        {"name": row[name_idx], "caption": row[caption_idx]}
        for row in rows
        if len(row) > name_idx
    ]


@router.get("/cubes/{cube}/measures", response_model=List[MetadataItem])
def list_cube_measures(cube: str):
    return service.list_measures(cube)


@router.get("/cubes/{cube}/dimensions", response_model=List[MetadataItem])
def list_cube_dimensions(cube: str):
    return service.list_dimensions(cube)


@router.get("/cubes/{cube}/levels", response_model=List[LevelInfo])
def list_cube_levels(cube: str, hierarchy: str):
    hierarchy = _normalize_hierarchy_ref(cube, hierarchy)
    return service.list_levels(cube, hierarchy)


@router.post("/query", response_model=QueryResponse)
def run_query(req: QueryRequest):
    try:
        if req.mdx:
            result = service.client.execute_mdx(req.mdx)
        else:
            measure = req.measure or (req.measures[0] if req.measures else None)
            hierarchy = req.hierarchy or (req.rows[0] if req.rows else None)
            if not measure or not hierarchy:
                raise HTTPException(status_code=400, detail="measure and hierarchy are required")
            
            # Normalize dimension-only references to full hierarchy references
            hierarchy = _normalize_hierarchy_ref(req.cube, hierarchy)
            
            result = service.query(req.cube, measure, hierarchy, req.level, req.where)

        return {"columns": result.get("columns", []), "rows": result.get("rows", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/pivot", response_model=QueryResponse)
def run_pivot(req: QueryRequest):
    """Pivot query: swap row and column hierarchies."""
    try:
        measure = req.measure or (req.measures[0] if req.measures else None)
        row_hierarchy = req.hierarchy or (req.rows[0] if req.rows else None)
        col_hierarchy = req.columns[0] if req.columns else None
        
        if not measure or not row_hierarchy or not col_hierarchy:
            raise HTTPException(status_code=400, detail="measure, row hierarchy (hierarchy), and column hierarchy (columns[0]) are required")
        
        # Normalize dimension-only references to full hierarchy references
        row_hierarchy = _normalize_hierarchy_ref(req.cube, row_hierarchy)
        col_hierarchy = _normalize_hierarchy_ref(req.cube, col_hierarchy)
        
        result = service.pivot(
            req.cube,
            measure,
            row_hierarchy,
            req.level,
            col_hierarchy,
            req.where,
        )
        
        return {"columns": result.get("columns", []), "rows": result.get("rows", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/drill", response_model=QueryResponse)
def drill(req: QueryRequest):
    if not req.parent_member and not req.drill_member:
        raise HTTPException(status_code=400, detail="parent_member is required")

    parent_member = req.parent_member or req.drill_member

    try:
        measure = req.measure or (req.measures[0] if req.measures else None)
        if not measure:
            raise HTTPException(status_code=400, detail="measure is required")

        # If the frontend sent a plain caption or numeric label for parent_member,
        # try to resolve it to a proper MDX unique member name using the provided parent_level.
        if parent_member and not parent_member.strip().startswith('['):
            if not req.parent_level:
                raise HTTPException(status_code=400, detail="parent_level is required when parent_member is not a MDX member expression")
            try:
                parent_member = service.resolve_member_unique_name(req.cube, req.parent_level, parent_member)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        if req.next_level:
            # Prefer .Children for one-level drills (safer); fall back to DESCENDANTS if needed
            try:
                result = service.drilldown_children(
                    req.cube,
                    measure,
                    parent_member,
                    req.where,
                    req.next_level,
                )
            except Exception:
                # fallback to descendants for multi-level or when .Children fails
                result = service.drilldown_descendants(
                    req.cube,
                    measure,
                    parent_member,
                    req.next_level,
                    req.where,
                )
        else:
            result = service.drilldown_children(
                req.cube,
                measure,
                parent_member,
                req.where,
            )

        return {
            "columns": result.get("columns", []),
            "rows": result.get("rows", [])
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
