"""Shared, isolated endpoint configuration for research and autonomous decisions."""
from config.settings import settings
from core.utils.openai_responses import openai_endpoint_targets


def research_agent_endpoint_targets(
    *,
    primary_model: str,
    backup_model: str = "",
    backup_first: bool = False,
) -> list[dict]:
    """Primary endpoint serving `primary_model`, then the backups serving `backup_model`.

    `backup_first` puts the first backup endpoint in front with `primary_model` and keeps the
    primary endpoint behind it with `backup_model` (e.g. a GPT model that only the backup relay
    serves, falling back to the primary relay's DeepSeek). Keys stay with their endpoints.
    """
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
    if backup_first and len(targets) > 1:
        first, fallback = dict(targets[1]), dict(targets[0])
        first["model"], fallback["model"] = targets[0]["model"], targets[1]["model"]
        targets = [first, fallback] + targets[2:]
        for index, target in enumerate(targets):
            target["index"], target["is_backup"] = index, index > 0
    for target in targets:
        target["force_chat_completions"] = bool(
            dedicated and settings.AI_MODEL_FORCE_CHAT_COMPLETIONS
        )
        # DeepSeek thinking shares the completion budget with the final JSON.
        # These bounded calls need that budget for the structured result.
        if dedicated and str(target.get("model") or "").lower().startswith("deepseek-v4"):
            target["chat_options"] = {"thinking": {"type": "disabled"}}
    return targets
