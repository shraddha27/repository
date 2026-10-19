import pytest

from app.clients import product_api


def test_page_size_is_configured():
    assert product_api.PAGE_SIZE > 0


def test_iter_products_fetches_pages_incrementally(monkeypatch):
    pages = iter(
        [
            {"products": [{"id": "P1"}], "has_next": True, "next_page": 2},
            {"products": [{"id": "P2"}], "has_next": False, "next_page": None},
        ]
    )
    requested_pages = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return next(pages)

    def request(method, url, **kwargs):
        requested_pages.append(kwargs["params"]["page"])
        return Response()

    monkeypatch.setattr(product_api, "request_with_retries", request)

    assert list(product_api.iter_products()) == [{"id": "P1"}, {"id": "P2"}]
    assert requested_pages == [1, 2]


def test_iter_products_rejects_invalid_pagination(monkeypatch):
    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"products": [], "has_next": True, "next_page": 1}

    monkeypatch.setattr(product_api, "request_with_retries", lambda *args, **kwargs: Response())

    with pytest.raises(ValueError, match="next_page"):
        list(product_api.iter_products())
