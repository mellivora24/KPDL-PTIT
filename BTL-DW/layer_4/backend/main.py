from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers.olap import router as olap_router

app = FastAPI(title="DW OLAP API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

app.include_router(olap_router, prefix="/api/olap")


@app.get("/")
def root():
	return {"message": "DW OLAP API - use /api/olap"}

