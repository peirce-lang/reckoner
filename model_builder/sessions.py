"""
model_builder/sessions.py — Upload session store for Model Builder.

In-memory dict keyed by UUID4 token. Tokens expire after SESSION_TTL_SECONDS.
Holds the parsed DataFrame and column metadata between wizard steps so the
file does not need to be re-sent on every request.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import HTTPException

from .config import SESSION_TTL_SECONDS


class SessionData:
    def __init__(
        self,
        df:          pd.DataFrame,
        source_info: Dict[str, Any],
        columns:     List[Dict],
    ):
        self.df          = df
        self.source_info = source_info   # { type, filename/table_name, format, ... }
        self.columns     = columns       # [{ name, samples, suggested_dim, suggested_key }]
        self.created_at  = time.time()
        self.mapping:    Optional[List[Dict]] = None   # set by /review


_sessions: Dict[str, SessionData] = {}


def new_token() -> str:
    return str(uuid.uuid4())


def get_session(token: str) -> SessionData:
    session = _sessions.get(token)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session '{token}' not found or expired.")
    if time.time() - session.created_at > SESSION_TTL_SECONDS:
        del _sessions[token]
        raise HTTPException(status_code=410, detail="Session expired. Please re-upload your file.")
    return session


def put_session(token: str, session: SessionData) -> None:
    _sessions[token] = session


def purge_expired() -> None:
    """Remove expired sessions. Called on each request — cheap enough at this scale."""
    now     = time.time()
    expired = [t for t, s in _sessions.items() if now - s.created_at > SESSION_TTL_SECONDS]
    for t in expired:
        del _sessions[t]
