"""
model_builder_api.py — Compatibility shim.

This file exists for backward compatibility only.
The implementation lives in the model_builder/ package.

reckoner_api.py imports this as:
    from model_builder_api import router as mb_router

Once Tauri and all integrations are confirmed stable on the new package,
update reckoner_api.py to:
    from model_builder.api import router as mb_router

and delete this file.
"""

from model_builder.api import router

__all__ = ["router"]
