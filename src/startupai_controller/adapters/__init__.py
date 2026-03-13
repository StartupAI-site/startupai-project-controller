"""Adapter implementations for port protocols.

Adapters wrap existing mechanism code (GitHub CLI, SQLite, subprocess)
behind typed port interfaces.

Dependency rule: adapters/ may import from domain/ and stdlib. They wrap
concrete mechanism modules directly and re-export adapter-internal types where
needed for stable adapter-level compatibility surfaces.
"""
