"""add rolling summaries

Revision ID: 0002_summaries
Revises: 0001_initial
Create Date: 2026-11-21

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0002_summaries"
down_revision: Union[str, None] = "0001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "summaries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("conversation_id", sa.String(length=64), nullable=False),
        sa.Column("up_to_message_id", sa.String(length=64), nullable=False),
        sa.Column("up_to_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("token_count", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_summaries_conv_created",
        "summaries",
        ["conversation_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_summaries_conv_created", table_name="summaries")
    op.drop_table("summaries")
