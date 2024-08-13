"""Excel target sink class, which handles writing streams."""

from __future__ import annotations

from target_excel.client import ExcelSink
from urllib.parse import urljoin

BASE_URL = "https://graph.microsoft.com"
URL = "users/{}/drive/root:/{}:/"

class FallbackSink(ExcelSink):
    """Excel target sink class."""
    max_size = 10000  # Max records to write in one batch
    table_id = None

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def endpoint(self) -> str:
        raise ""

    @property
    def base_url(self) -> str:
        url_suffix = URL.format(self.config.get("user_email_id"), 
                                self.config.get('workbook_id_path'))        
        return urljoin(BASE_URL, self.config.get('api-version', "v1.0")) + "/" + url_suffix

    @property
    def unified_schema(self):
        return None

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        resp = self._request("get", "workbook/worksheets/")
        worksheets = [x["name"] for x in resp.json().get("value")]

        # If the sheet does not already exists, we need to create
        if self.stream_name not in worksheets:
            resp = self._request("post", "workbook/worksheets/add", request_data={
                "name": self.stream_name
            })
            self.logger.info(f"Added worksheet {self.stream_name} to workbook. Status code={resp.status_code}")


    def handle_batch_response(self, response):
        results = []

        if response.status_code == 201:
            results.append({"success": True})
        else:
            results.append({"success": False})

        return {"state_updates": results}


    def make_batch_request(self, records):
        header = list(records[0].keys())

        # If the table does not already exist, we need to create
        resp = self._request("get", f"workbook/worksheets/{self.stream_name}/tables")
        tables = resp.json().get("value")

        if len(tables) == 0:
            resp = self._request("post", f"workbook/worksheets/{self.stream_name}/tables/add", request_data={
                "address": f"{self.stream_name}!A1:{chr(ord('@')+len(header)-1)}{len(records)}",
                "hasHeaders": False
            }).json()
            self.table_id = resp.get("id")
            self.logger.info(f"Added table {self.table_id} to worksheet.")

            # Set the column names
            columns = self._request("get", f"workbook/worksheets/{self.stream_name}/tables/{self.table_id}/columns").json().get("value")
            for i in range(len(columns)):
                c = columns[i]
                resp = self._request("patch", f"workbook/worksheets/{self.stream_name}/tables/{self.table_id}/columns/{c['id']}", request_data={
                    "name": header[i]
                }).json()
        else:
            self.table_id = tables[0].get("id")

        # Create the records
        records = [list(r.values()) for r in records]
        resp = self._request("post", f"workbook/worksheets/{self.stream_name}/tables/{self.table_id}/rows", request_data={
            "values": records
        })
        return resp
