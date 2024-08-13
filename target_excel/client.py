"""Excel target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from target_hotglue.client import HotglueBatchSink


from target_excel.auth import ExcelAuthenticator


class ExcelSink(HotglueBatchSink):
    """Excel target sink class."""

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers = self.http_headers
        auth_headers = self.authenticator.auth_headers

        for k in auth_headers:
            headers[k] = auth_headers[k]

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )
        self.validate_response(response)
        return response

    max_size = 10000  # Max records to write in one batch

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    @property
    def authenticator(self):
        url = "https://login.microsoftonline.com/common/oauth2/token"
        return ExcelAuthenticator(
            self._target,
            dict(), # TODO: Do I need to use this?
            url
        )