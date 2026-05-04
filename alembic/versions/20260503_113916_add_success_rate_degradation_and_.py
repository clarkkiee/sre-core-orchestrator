"""add_success_rate_degradation_and_latency_p99_degradation

Revision ID: dffb450c3a10
Revises: 38568570e523
Create Date: 2026-05-03 11:39:16.944761+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dffb450c3a10'
down_revision: Union[str, None] = '38568570e523'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.execute("ALTER TYPE isoindicator ADD VALUE IF NOT EXISTS 'SUCCESS_RATE_DEGRADATION'")
    op.execute("ALTER TYPE isoindicator ADD VALUE IF NOT EXISTS 'LATENCY_P99_DEGRADATION'")

def downgrade() -> None:
    op.execute(
        "DELETE FROM evaluation_indicators "
        "WHERE indicator IN ('SUCCESS_RATE_DEGRADATION', 'LATENCY_P99_DEGRADATION')"
    )