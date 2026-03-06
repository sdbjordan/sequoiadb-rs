"""CollectionSpace handle."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import SdbClient

from .collection import Collection


class CollectionSpace:
    def __init__(self, client: SdbClient, name: str):
        self._client = client
        self.name = name

    def get_collection(self, cl_name: str) -> Collection:
        full_name = f"{self.name}.{cl_name}"
        return Collection(self._client, full_name)

    def create_collection(self, cl_name: str) -> Collection:
        self._client.create_collection(self.name, cl_name)
        return self.get_collection(cl_name)

    def drop_collection(self, cl_name: str) -> None:
        self._client.drop_collection(self.name, cl_name)
