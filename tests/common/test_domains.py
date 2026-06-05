"""Unit tests for PSL domain normalization."""

import pytest

from spark_jobs.common.domains import registered_domain


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("example.com", "example.com"),
        ("www.facebook.com", "facebook.com"),
        ("fonts.googleapis.com", "googleapis.com"),
        ("bbc.co.uk", "bbc.co.uk"),
        ("www.bbc.co.uk", "bbc.co.uk"),
        ("a.b.c.example.com", "example.com"),
        ("WWW.Example.COM", "example.com"),
        ("example.com.", "example.com"),
    ],
)
def test_registered_domain_normalizes_to_registered(host: str, expected: str) -> None:
    assert registered_domain(host) == expected


@pytest.mark.parametrize(
    "bad",
    [
        None,
        "",
        "   ",
        "8.8.8.8",
        "2001:4860:4860::8888",
        "foo.localhost",
        "host.local",
        "bare",
        "a\x00b.com",
    ],
)
def test_registered_domain_rejects_invalid(bad: str | None) -> None:
    assert registered_domain(bad) is None


def test_registered_domain_idna_encodes_unicode() -> None:
    assert registered_domain("münchen.de") == "xn--mnchen-3ya.de"
