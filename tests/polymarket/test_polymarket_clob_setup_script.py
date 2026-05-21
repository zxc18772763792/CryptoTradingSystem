from __future__ import annotations

from pathlib import Path

from scripts import polymarket_clob_setup as setup


def test_parse_key_value_file_supports_colon_and_equals(tmp_path: Path):
    path = tmp_path / "keys.txt"
    path.write_text(
        "\n".join(
            [
                "# comment",
                "RELAYER_API_KEY: abc123",
                "export CLOB_API_KEY=key-1",
                "CLOB_SECRET = 'secret-1'",
                "bad line",
            ]
        ),
        encoding="utf-8",
    )

    values = setup.parse_key_value_file(path)

    assert values["RELAYER_API_KEY"] == "abc123"
    assert values["CLOB_API_KEY"] == "key-1"
    assert values["CLOB_SECRET"] == "secret-1"
    assert "bad line" not in values


def test_mask_secret_keeps_prefix_and_suffix():
    assert setup.mask_secret("abcdefghijklmnopqrstuvwxyz") == "abcd...wxyz"
    assert setup.mask_secret("short") == "*****"


def test_normalize_api_creds_accepts_sdk_style_dict():
    creds = setup.normalize_api_creds(
        {
            "apiKey": "key",
            "secret": "secret",
            "passphrase": "pass",
        }
    )

    assert creds == {
        "CLOB_API_KEY": "key",
        "CLOB_SECRET": "secret",
        "CLOB_PASS_PHRASE": "pass",
    }


def test_parse_key_value_file_maps_legacy_relayer_aliases(tmp_path: Path):
    path = tmp_path / "keys.txt"
    path.write_text(
        "\n".join(
            [
                "https://example.com/not-a-key",
                "address: 0xA8BA09b98Cec9F146256eC66538444b9c618484a",
                "key: 019e111111111111111111111111111111",
            ]
        ),
        encoding="utf-8",
    )

    values = setup.parse_key_value_file(path)

    assert values["RELAYER_API_KEY_ADDRESS"] == "0xA8BA09b98Cec9F146256eC66538444b9c618484a"
    assert values["RELAYER_API_KEY"].startswith("019e")
    assert "https" not in values
