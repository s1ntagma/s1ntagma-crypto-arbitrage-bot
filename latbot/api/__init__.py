"""HTTP + WebSocket API for the latbot terminal UI.

``create_app(ctx)`` builds the FastAPI application; ``build_snapshot(ctx)``
is the payload every WebSocket client receives on connect.
"""
from .server import WsHub, build_snapshot, build_state, create_app, serialise_event

__all__ = ["WsHub", "build_snapshot", "build_state", "create_app", "serialise_event"]
