"""Adapter implementations for port protocols.

Adapters wrap existing mechanism code (GitHub CLI, SQLite, subprocess)
behind typed port interfaces.

Dependency rule: adapters/ may import from domain/ and stdlib. They wrap
existing modules (board_io, consumer_db, github_http) and re-export
adapter-internal types.
"""
