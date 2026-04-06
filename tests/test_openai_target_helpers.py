from core.utils.openai_responses import openai_endpoint_targets


def test_openai_endpoint_targets_support_multiple_backup_api_keys():
    targets = openai_endpoint_targets(
        primary_base_url="https://primary.test/v1",
        backup_base_urls="https://secondary.test/v1,https://tertiary.test/v1",
        primary_api_key="primary-key",
        backup_api_key="secondary-key,tertiary-key",
    )

    assert [target["base_url"] for target in targets] == [
        "https://primary.test/v1",
        "https://secondary.test/v1",
        "https://tertiary.test/v1",
    ]
    assert [target["api_key"] for target in targets] == [
        "primary-key",
        "secondary-key",
        "tertiary-key",
    ]
