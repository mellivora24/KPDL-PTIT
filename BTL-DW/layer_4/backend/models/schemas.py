from typing import List, Optional, Any
from pydantic import BaseModel


class QueryRequest(BaseModel):
	cube: str
	measure: Optional[str] = None
	hierarchy: Optional[str] = None
	level: Optional[str] = None
	parent_level: Optional[str] = None
	parent_member: Optional[str] = None
	next_level: Optional[str] = None
	measures: Optional[List[str]] = None
	rows: Optional[List[str]] = None
	columns: Optional[List[str]] = None
	where: Optional[str] = None
	mdx: Optional[str] = None
	operation: Optional[str] = None
	drill_member: Optional[str] = None


class QueryResponse(BaseModel):
	columns: List[str]
	rows: List[List[Any]]


class CubeInfo(BaseModel):
	name: str
	caption: Optional[str]


class MetadataItem(BaseModel):
	name: str
	caption: Optional[str] = None
	unique_name: Optional[str] = None
	default_hierarchy: Optional[str] = None


class LevelInfo(BaseModel):
	name: str
	unique_name: str
	caption: Optional[str] = None
	number: Optional[int] = None

