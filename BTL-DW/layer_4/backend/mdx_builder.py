from typing import List, Optional


def normalize_cube(cube: str) -> str:
	return cube if cube.startswith("[") else f"[{cube}]"


def validate_hierarchy(hierarchy: str, param_name: str = "hierarchy") -> str:
	"""
	Validate that hierarchy is a full hierarchy reference (e.g., [Dim].[Hier]), not just a dimension.
	
	Raises ValueError if hierarchy looks like just a dimension name (no hierarchy specified).
	Returns the hierarchy unchanged if valid.
	"""
	if not hierarchy or not hierarchy.startswith("["):
		raise ValueError(f"{param_name} must start with '[': {hierarchy}")
	
	# Check if it's a full hierarchy reference: [Dim].[Hier]
	# If there's no second '[', it's likely just a dimension name
	first_bracket_close = hierarchy.find("]")
	if first_bracket_close == -1:
		raise ValueError(f"{param_name} has invalid format: {hierarchy}")
	
	# If the next character after first ] is not '.', it's just a dimension
	if first_bracket_close + 1 >= len(hierarchy) or hierarchy[first_bracket_close + 1] != ".":
		raise ValueError(
			f"{param_name} appears to be a dimension name only: {hierarchy}. "
			f"Multi-hierarchy dimensions require explicit hierarchy reference, e.g., [Dimension].[HierarchyName]"
		)
	
	return hierarchy


def build_simple_mdx(
	cube: str,
	measure: str,
	hierarchy: str,
	level: Optional[str] = None,
	where: Optional[str] = None,
	limit: int = 20,
) -> str:
	"""Build an MDX query for a dashboard card/chart view."""
	measure_part = measure or "[Measures].[Doanh Thu]"
	if not level:
		# Validate hierarchy if we're about to use it directly
		validate_hierarchy(hierarchy, "hierarchy")
	row_set = f"{level}.Members" if level else f"{hierarchy}.Members"
	where_clause = f"WHERE ({where})" if where else ""
	mdx = (
		f"SELECT {{ {measure_part} }} ON COLUMNS, "
		f"NON EMPTY {{ {row_set} }} DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION ON ROWS "
		f"FROM {normalize_cube(cube)} {where_clause}"
	)
	if limit and limit > 0:
		# Ensure MEMBER_UNIQUE_NAME and MEMBER_CAPTION are returned when using TOPCOUNT
		mdx = (
			f"SELECT TOPCOUNT({row_set}, {limit}, {measure_part}) ON ROWS, {{ {measure_part} }} ON COLUMNS "
			f"FROM {normalize_cube(cube)} {where_clause} "
		)
		# Append DIMENSION PROPERTIES clause to expose unique name and caption for rows
		mdx = mdx.replace(f"TOPCOUNT({row_set}, {limit}, {measure_part}) ON ROWS, {{ {measure_part} }} ON COLUMNS ",
		                  f"TOPCOUNT({row_set}, {limit}, {measure_part}) DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION ON ROWS, {{ {measure_part} }} ON COLUMNS ")
	return mdx


def build_drilldown_mdx(
	cube: str,
	measure: str,
	parent_member: str,
	next_level: str,
	where: Optional[str] = None,
) -> str:
	measure_part = measure or "[Measures].[Doanh Thu]"
	where_clause = f"WHERE ({where})" if where else ""
	# For a single-level drill (parent -> immediate children) use the .Children function
	# This avoids passing a level object/string to Descendants which can cause the
	# "expects a member expression" error if the argument is not a proper member.
	# If callers need multi-level descendants later, implement a distinct path.
	parent = parent_member.strip()
	return (
		f"SELECT {{ {measure_part} }} ON COLUMNS, "
		f"NON EMPTY {{ {parent}.Children }} "
		f"DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION ON ROWS "
		f"FROM {normalize_cube(cube)} {where_clause}"
	)


def build_pivot_mdx(
	cube: str,
	measure: str,
	row_hierarchy: str,
	row_level: Optional[str],
	col_hierarchy: str,
	col_level: Optional[str],
	where: Optional[str] = None,
) -> str:
	"""Build an MDX query with pivoted axes (swap rows and columns).
	
	Standard layout: measure on COLUMNS, row_hierarchy on ROWS
	Pivot layout: row_hierarchy on COLUMNS, col_hierarchy on ROWS
	(In reality, rows/columns swap: col_hierarchy members become rows, row_hierarchy members become columns.)
	"""
	measure_part = measure or "[Measures].[Doanh Thu]"
	if not row_level:
		validate_hierarchy(row_hierarchy, "row_hierarchy")
	if not col_level:
		validate_hierarchy(col_hierarchy, "col_hierarchy")
	row_set = f"{row_level}.Members" if row_level else f"{row_hierarchy}.Members"
	col_set = f"{col_level}.Members" if col_level else f"{col_hierarchy}.Members"
	where_clause = f"WHERE ({where})" if where else ""
	
	# Pivot: col_hierarchy on COLUMNS (horizontal members), measure on ROWS (vertical measure)
	# Data will be extracted by frontend and transposed for display
	return (
		f"SELECT {{ {row_set} }} DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION ON COLUMNS, "
		f"NON EMPTY {{ {col_set} }} DIMENSION PROPERTIES MEMBER_UNIQUE_NAME, MEMBER_CAPTION ON ROWS "
		f"FROM {normalize_cube(cube)} {where_clause}"
	)
