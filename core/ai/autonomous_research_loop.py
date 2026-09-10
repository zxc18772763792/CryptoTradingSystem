"""Public entry point for the bounded autonomous research service."""
from core.ai.research_loop_service import AutonomousResearchLoop, ResearchLoopConfig, get_research_loop

__all__ = ["AutonomousResearchLoop", "ResearchLoopConfig", "get_research_loop"]
