from typing import List, Optional


def normalize_cube(cube: str) -> str:
	return cube if cube.startswith("[") else f"[{cube}]"


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

