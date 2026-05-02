from fastapi import APIRouter, HTTPException
from typing import List

from models.schemas import QueryRequest, QueryResponse, CubeInfo, MetadataItem, LevelInfo
from services.olap_service import OLAPService

router = APIRouter()
service = OLAPService()


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
            result = service.query(req.cube, measure, hierarchy, req.level, req.where)

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
