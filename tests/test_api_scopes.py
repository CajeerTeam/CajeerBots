from core.api_routes import KNOWN_SCOPES, ROUTES, openapi_document


def test_every_non_public_route_has_known_scope():
    for route in ROUTES:
        assert route.auth_scope
        assert route.auth_scope in KNOWN_SCOPES


def test_openapi_exposes_auth_scopes():
    doc = openapi_document("0.test", "contract.test")
    assert "x-known-scopes" in doc
    for route in ROUTES:
        operation = doc["paths"][route.path][route.method.lower()]
        assert operation["x-auth-scope"] == route.auth_scope
        assert operation["operationId"]
