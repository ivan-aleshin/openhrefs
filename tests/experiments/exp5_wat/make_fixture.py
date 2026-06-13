"""One-shot generator for the synthetic WAT test fixture."""

import json
from pathlib import Path

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

_OUT = Path(__file__).parent / "fixtures" / "sample.wat.gz"


def _record(writer, target_uri: str, links: list[dict]):
    payload = json.dumps(
        {
            "Envelope": {
                "WARC-Header-Metadata": {"WARC-Target-URI": target_uri},
                "Payload-Metadata": {"HTTP-Response-Metadata": {"HTML-Metadata": {"Links": links}}},
            }
        }
    ).encode("utf-8")
    headers = StatusAndHeaders("", [], protocol="")
    return writer.create_warc_record(
        target_uri,
        "metadata",
        payload=__import__("io").BytesIO(payload),
        http_headers=headers,
        warc_content_type="application/json",
    )


def main() -> None:
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    with _OUT.open("wb") as fh:
        writer = WARCWriter(fh, gzip=True)
        writer.write_record(
            _record(
                writer,
                "http://src1.com/page",
                [
                    {"path": "A@/href", "url": "https://a.bg/x", "text": "buy", "rel": "nofollow"},
                    {"path": "IMG@/src", "url": "https://a.bg/i.png"},
                ],
            )
        )
        writer.write_record(
            _record(
                writer,
                "http://src1.com/other",
                [{"path": "A@/href", "url": "https://x.net/y", "text": "out"}],
            )
        )


if __name__ == "__main__":
    main()
