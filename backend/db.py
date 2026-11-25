import json
import os
import uuid
from datetime import datetime

import asyncpg

class EmailTaken(Exception):
    pass


def _json_ready(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class Database:
    def __init__(self) -> None:
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        url = os.getenv("DATABASE_URL", "")
        if not url:
            raise RuntimeError("DATABASE_URL is not configured.")
        try:
            self.pool = await asyncpg.create_pool(url, min_size=1, max_size=5, init=self._init_connection)
        except Exception as exc:
            raise RuntimeError(
                "Could not connect to PostgreSQL. Start it with 'docker compose up -d' and check DATABASE_URL."
            ) from exc
        await self._create_tables()

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()

    async def _init_connection(self, conn: asyncpg.Connection) -> None:
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )

    async def _create_tables(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analyses (
                    id UUID PRIMARY KEY,
                    user_id UUID NOT NULL REFERENCES users(id),
                    region TEXT NOT NULL,
                    resources_scanned INT NOT NULL DEFAULT 0,
                    issues_found INT NOT NULL DEFAULT 0,
                    estimated_savings TEXT,
                    analysis_result JSONB,
                    status TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

    async def create_user(self, email: str, password_hash: str) -> dict:
        assert self.pool is not None
        user_id = uuid.uuid4()
        try:
            row = await self.pool.fetchrow(
                """
                INSERT INTO users (id, email, password_hash)
                VALUES ($1, $2, $3)
                RETURNING id, email, created_at
                """,
                user_id,
                email,
                password_hash,
            )
        except asyncpg.UniqueViolationError as exc:
            raise EmailTaken() from exc
        return self._user(row)

    async def get_user_by_email(self, email: str) -> dict | None:
        assert self.pool is not None
        row = await self.pool.fetchrow(
            "SELECT id, email, password_hash, created_at FROM users WHERE email = $1",
            email,
        )
        if row is None:
            return None
        user = self._user(row)
        user["password_hash"] = row["password_hash"]
        return user

    async def get_user_by_id(self, user_id: str) -> dict | None:
        assert self.pool is not None
        row = await self.pool.fetchrow(
            "SELECT id, email, created_at FROM users WHERE id = $1",
            uuid.UUID(user_id),
        )
        return None if row is None else self._user(row)

    async def create_analysis(self, user_id: str, region: str) -> str:
        assert self.pool is not None
        analysis_id = uuid.uuid4()
        await self.pool.execute(
            """
            INSERT INTO analyses (id, user_id, region, status)
            VALUES ($1, $2, $3, 'running')
            """,
            analysis_id,
            uuid.UUID(user_id),
            region,
        )
        return str(analysis_id)

    async def complete_analysis(
        self,
        analysis_id: str,
        user_id: str,
        resources_scanned: int,
        issues_found: int,
        estimated_savings: str,
        analysis_result: dict,
    ) -> None:
        assert self.pool is not None
        await self.pool.execute(
            """
            UPDATE analyses
            SET resources_scanned = $1,
                issues_found = $2,
                estimated_savings = $3,
                analysis_result = $4,
                status = 'complete'
            WHERE id = $5 AND user_id = $6
            """,
            resources_scanned,
            issues_found,
            estimated_savings,
            analysis_result,
            uuid.UUID(analysis_id),
            uuid.UUID(user_id),
        )

    async def fail_analysis(self, analysis_id: str, user_id: str, message: str) -> None:
        assert self.pool is not None
        await self.pool.execute(
            """
            UPDATE analyses
            SET status = 'failed',
                analysis_result = $1
            WHERE id = $2 AND user_id = $3
            """,
            {"error": message},
            uuid.UUID(analysis_id),
            uuid.UUID(user_id),
        )

    async def list_analyses(self, user_id: str) -> list[dict]:
        assert self.pool is not None
        rows = await self.pool.fetch(
            """
            SELECT id, region, resources_scanned, issues_found, estimated_savings, status, created_at
            FROM analyses
            WHERE user_id = $1
            ORDER BY created_at DESC
            """,
            uuid.UUID(user_id),
        )
        return [self._analysis(row) for row in rows]

    async def get_analysis(self, analysis_id: str, user_id: str) -> dict | None:
        assert self.pool is not None
        try:
            parsed_id = uuid.UUID(analysis_id)
        except ValueError:
            return None
        row = await self.pool.fetchrow(
            """
            SELECT id, region, resources_scanned, issues_found, estimated_savings,
                   analysis_result, status, created_at
            FROM analyses
            WHERE id = $1 AND user_id = $2
            """,
            parsed_id,
            uuid.UUID(user_id),
        )
        return None if row is None else self._analysis(row)

    def _user(self, row: asyncpg.Record) -> dict:
        return {"id": str(row["id"]), "email": row["email"], "created_at": _json_ready(row["created_at"])}

    def _analysis(self, row: asyncpg.Record) -> dict:
        result = {
            "id": str(row["id"]),
            "region": row["region"],
            "resources_scanned": row["resources_scanned"],
            "issues_found": row["issues_found"],
            "estimated_savings": row["estimated_savings"],
            "status": row["status"],
            "created_at": _json_ready(row["created_at"]),
        }
        if "analysis_result" in row.keys():
            result["analysis_result"] = row["analysis_result"]
        return result


db = Database()
