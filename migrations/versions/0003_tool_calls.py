"""add tool call audit table

Revision ID: 0003_tool_calls
Revises: 0002_summaries
Create Date: 2026-11-22

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003_tool_calls"
down_revision: Union[str, None] = "0002_summaries"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tool_calls",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("call_id", sa.String(length=128), nullable=False),
        sa.Column("tool_name", sa.String(length=128), nullable=False),
        sa.Column("server_name", sa.String(length=64), nullable=True),
        sa.Column("step", sa.Integer(), server_default="1", nullable=False),
        sa.Column("arguments_json", sa.JSON(), nullable=True),
        sa.Column("result_json", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["run_id"], ["runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id", "call_id", name="uq_tool_calls_run_call"),
    )
    op.create_index("idx_tool_calls_run", "tool_calls", ["run_id"])


def downgrade() -> None:
    op.drop_index("idx_tool_calls_run", table_name="tool_calls")
    op.drop_table("tool_calls")
