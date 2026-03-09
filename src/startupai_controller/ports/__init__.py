"""Port protocols — use-case-oriented interfaces for domain isolation.

Ports define typed contracts between domain/orchestration and external
mechanisms. Adapters implement these protocols.

Dependency rule: ports/ imports only from domain/ and stdlib.
"""
