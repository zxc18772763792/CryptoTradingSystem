"""Shared, isolated endpoint configuration for research and autonomous decisions."""
from config.settings import settings
from core.utils.openai_responses import openai_endpoint_targets


def research_agent_endpoint_targets(*, primary_model: str, backup_model: str = "") -> list[dict]:
    dedicated = bool(settings.AI_MODEL_BASE_URL.strip())
    prefix = "AI_MODEL_" if dedicated else "OPENAI_"
    targets = openai_endpoint_targets(
        primary_base_url=getattr(settings, prefix + "BASE_URL"),
        primary_api_key=getattr(settings, prefix + "API_KEY"),
        backup_base_urls=getattr(settings, prefix + "BACKUP_BASE_URL"),
        backup_api_key=getattr(settings, prefix + "BACKUP_API_KEY"),
        primary_model=primary_model,
        backup_model=backup_model or getattr(settings, prefix + "BACKUP_MODEL"),
    )
    for target in targets:
        target["force_chat_completions"] = bool(
            dedicated and settings.AI_MODEL_FORCE_CHAT_COMPLETIONS
        )
        # DeepSeek thinking shares the completion budget with the final JSON.
        # These bounded calls need that budget for the structured result.
        if dedicated and str(target.get("model") or "").lower().startswith("deepseek-v4"):
            target["chat_options"] = {"thinking": {"type": "disabled"}}
    return targets
