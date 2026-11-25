"""add long-term memories with pgvector

Revision ID: 0004_memories
Revises: 0003_tool_calls
Create Date: 2026-11-22

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004_memories"
down_revision: Union[str, None] = "0003_tool_calls"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "memories",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("embedding", sa.Text(), nullable=True),
        sa.Column("source_message_id", sa.String(length=64), nullable=True),
        sa.Column("confidence", sa.Float(), server_default="0.8", nullable=False),
        sa.Column("importance", sa.Float(), server_default="0.5", nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default="active",
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "content_hash",
            name="uq_memories_user_hash",
        ),
    )
    # The ORM-friendly column above is TEXT in SQLite; on PostgreSQL it must be
    # a real pgvector column for HNSW + cosine distance.
    op.execute(
        "ALTER TABLE memories ALTER COLUMN embedding "
        "TYPE vector(1024) USING embedding::vector(1024)"
    )
    op.create_index(
        "idx_memories_user_status",
        "memories",
        ["user_id", "status"],
    )
    op.execute(
        "CREATE INDEX idx_memories_embedding "
        "ON memories USING hnsw (embedding vector_cosine_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_memories_embedding")
    op.drop_index("idx_memories_user_status", table_name="memories")
    op.drop_table("memories")
