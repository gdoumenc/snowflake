import typing as t

from pydantic import BaseModel
from pydantic import ConfigDict
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.exc import NoResultFound

from .data import CursorPagination
from .data import JsonApiDataMixin


class Pagination(t.Protocol, t.Iterable):
    total: int
    page: int
    pages: int
    per_page: int
    has_prev: bool
    prev_num: int | None
    has_next: bool
    next_num: int | None


@t.runtime_checkable
class Scalar(t.Protocol):
    def one(self) -> JsonApiDataMixin:
        ...


@t.runtime_checkable
class Query(t.Protocol):
    def paginate(self, *, page, per_page, max_per_page) -> type[Pagination]:
        ...

    def all(self) -> list[JsonApiDataMixin]:
        ...

    def one(self) -> JsonApiDataMixin:
        ...


class ListPagination(CursorPagination):
    total: int = 0
    values: list[t.Any]

    def __init__(self, **data):
        super().__init__(**data)
        self.total = len(self.values)

    def __iter__(self):
        assert self.page is not None  # by the validator
        assert self.per_page is not None  # by the validator
        return self.values[(self.page - 1) * self.per_page: self.page * self.per_page].__iter__()

    @classmethod
    def paginate(cls, values) -> Pagination:
        pagination = cls(values=values, page=1, per_page=len(values))
        return t.cast(Pagination, pagination)


class ListQuery(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    values: list[JsonApiDataMixin]

    def paginate(self, *, page, per_page, max_per_page) -> Pagination:
        return ListPagination(values=self.values, page=page, per_page=per_page)  # type: ignore[return-value]

    def all(self) -> list[JsonApiDataMixin]:
        return self.values

    def one(self) -> JsonApiDataMixin:
        if len(self.values) == 0:
            raise NoResultFound()
        if len(self.values) > 1:
            raise MultipleResultsFound()
        return self.values[0]
