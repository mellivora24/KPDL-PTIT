import os
from pathlib import Path
import requests
import xml.etree.ElementTree as ET
from typing import Dict, Any

from dotenv import load_dotenv
import clr


load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")


def _load_adomd_reference() -> None:
	"""Load the ADOMD.NET assembly before importing pyadomd.

	The Windows-auth flow depends on Microsoft.AnalysisServices.AdomdClient.dll.
	"""
	candidate_paths = []
	env_path = os.getenv("SSAS_ADOMD_DLL")
	if env_path:
		candidate_paths.append(Path(env_path))

	candidate_paths.extend([
		Path(r"C:\Program Files\Microsoft.NET\ADOMD.NET\160\Microsoft.AnalysisServices.AdomdClient.dll"),
		Path(r"C:\Program Files (x86)\Microsoft.NET\ADOMD.NET\160\Microsoft.AnalysisServices.AdomdClient.dll"),
		Path(r"C:\Windows\Microsoft.NET\assembly\GAC_MSIL\Microsoft.AnalysisServices.AdomdClient\v4.0_15.0.0.0__89845dcd8080cc91\Microsoft.AnalysisServices.AdomdClient.dll"),
	])

	for candidate in candidate_paths:
		if candidate and candidate.exists():
			clr.AddReference(str(candidate))
			return

	raise RuntimeError(
		"Không tìm thấy Microsoft.AnalysisServices.AdomdClient.dll. "
		"Hãy cài ADOMD.NET hoặc đặt SSAS_ADOMD_DLL trong .env."
	)


class SSASClient:
	"""SSAS client with Windows Integrated Authentication support and a mock fallback."""

	def __init__(self, xmla_url: str = None, username: str = None, password: str = None):
		self.connection_mode = os.getenv("SSAS_CONNECTION_MODE", "windows").lower()
		self.xmla_url = xmla_url or os.getenv("SSAS_XMLA_URL")
		self.server = os.getenv("SSAS_SERVER", "localhost")
		self.catalog = os.getenv("SSAS_CATALOG", "DW_SSAS")
		self.auth = (username, password) if username and password else None

	def execute_mdx(self, mdx: str) -> Dict[str, Any]:
		"""Execute MDX against SSAS.

		Returns: {"columns": [...], "rows": [[...], ...]}
		"""
		if self.connection_mode == "windows":
			return self._execute_with_windows_auth(mdx)

		if not self.xmla_url:
			raise RuntimeError("SSAS_XMLA_URL is required when SSAS_CONNECTION_MODE is not windows")

		envelope = f'''<?xml version="1.0" encoding="utf-8"?>
			<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
				<soap:Body>
					<Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
						<Command>
							<Statement>{mdx}</Statement>
						</Command>
						<Properties>
							<PropertyList>
								<Catalog>{self.catalog}</Catalog>
							</PropertyList>
						</Properties>
					</Execute>
				</soap:Body>
			</soap:Envelope>'''

		headers = {"Content-Type": "text/xml; charset=utf-8"}
		resp = requests.post(self.xmla_url, data=envelope.encode("utf-8"), headers=headers, auth=self.auth, timeout=30)
		resp.raise_for_status()

		return self._parse_xmla_response(resp.content)

	def _execute_with_windows_auth(self, mdx: str) -> Dict[str, Any]:
		try:
			_load_adomd_reference()
			from pyadomd import Pyadomd
			connection_string = (
				f"Provider=MSOLAP;Data Source={self.server};"
				f"Initial Catalog={self.catalog};Integrated Security=SSPI;"
			)

			with Pyadomd(connection_string) as conn:
				cursor = conn.cursor()
				cursor.execute(mdx)
				rows = cursor.fetchall()
				columns = [column[0] for column in cursor.description] if cursor.description else []
				return {
					"columns": columns,
					"rows": [list(row) for row in rows],
				}
		except Exception as exc:
			raise RuntimeError(f"Không đọc được dữ liệu SSAS bằng Windows auth: {exc}") from exc

	def _parse_xmla_response(self, xml_bytes: bytes) -> Dict[str, Any]:
		root = ET.fromstring(xml_bytes)

		rows = []
		columns = []
		row_elems = root.findall(".//row")
		if row_elems:
			for row in row_elems:
				if not columns:
					columns = [cell.tag for cell in list(row)]
				rows.append([cell.text for cell in list(row)])
			return {"columns": columns, "rows": rows}

		return {"columns": ["_raw"], "rows": [[root.text or ""]]}



