"""add_response_time_p95_and_latency_p95_degradation

Revision ID: f1c2a8d4e9b0
Revises: 256a803dbcd6
Create Date: 2026-05-08 12:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1c2a8d4e9b0'
down_revision: Union[str, None] = '256a803dbcd6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE isoindicator ADD VALUE IF NOT EXISTS 'RESPONSE_TIME_P95'")
    op.execute("ALTER TYPE isoindicator ADD VALUE IF NOT EXISTS 'LATENCY_P95_DEGRADATION'")


def downgrade() -> None:
    op.execute(
        "DELETE FROM evaluation_indicators "
        "WHERE indicator IN ('RESPONSE_TIME_P95', 'LATENCY_P95_DEGRADATION')"
    )
