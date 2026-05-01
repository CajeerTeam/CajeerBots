from core.proxy import client_ip_from_headers


def test_real_ip_requires_trusted_proxy():
    headers = {"x-forwarded-for": "203.0.113.10, 127.0.0.1"}
    assert client_ip_from_headers(remote_ip="127.0.0.1", headers=headers, behind_reverse_proxy=True, trusted_proxy_cidrs=["127.0.0.1/32"], real_ip_header="X-Forwarded-For") == "203.0.113.10"
    assert client_ip_from_headers(remote_ip="198.51.100.1", headers=headers, behind_reverse_proxy=True, trusted_proxy_cidrs=["127.0.0.1/32"], real_ip_header="X-Forwarded-For") == "198.51.100.1"
