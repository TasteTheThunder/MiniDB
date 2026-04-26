"""Normalization package entry points."""

from .schema_analyzer import analyze_schema
from .fd_manager import get_fds, save_fds

__all__ = ["analyze_schema", "get_fds", "save_fds"]
