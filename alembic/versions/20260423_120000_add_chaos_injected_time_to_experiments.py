"""add_chaos_injected_time_to_experiments

Stores the authoritative fault injection timestamp (from chaos-exporter) directly
on the experiment row so evaluation can use it without a post-hoc VM query race.

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
Create Date: 2026-04-23 12:00:00.000000+00:00

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c2d3e4f5a6b7"
down_revision: Union[str, None] = "b1c2d3e4f5a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("chaos_experiments", sa.Column("chaos_injected_time", sa.Float(), nullable=True))
    op.add_column("chaos_experiments", sa.Column("baseline_start", sa.Float(), nullable=True))
    op.add_column("chaos_experiments", sa.Column("baseline_end", sa.Float(), nullable=True))
    op.add_column("chaos_experiments", sa.Column("recovery_start", sa.Float(), nullable=True))
    op.add_column("chaos_experiments", sa.Column("recovery_end", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("chaos_experiments", "chaos_injected_time")
    op.drop_column("chaos_experiments", "baseline_start")
    op.drop_column("chaos_experiments", "baseline_end")
    op.drop_column("chaos_experiments", "recovery_start")
    op.drop_column("chaos_experiments", "recovery_end")
    
