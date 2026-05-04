"""add_fault_tolerance_ratio_and_p99_indicators

Adds FAULT_TOLERANCE_RATIO and RESPONSE_TIME_P99 to the isoindicator enum.
RESPONSE_TIME_P95 and MEAN_TIME_TO_FAILURE are retained in the enum for
backward compatibility but no new rows will be created for them.

Revision ID: 38568570e523
Revises: d63d9e2ec25c
Create Date: 2026-05-02 16:57:21.391421+00:00

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '38568570e523'
down_revision: Union[str, None] = 'd63d9e2ec25c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE isoindicator ADD VALUE IF NOT EXISTS 'FAULT_TOLERANCE_RATIO'")
    op.execute("ALTER TYPE isoindicator ADD VALUE IF NOT EXISTS 'RESPONSE_TIME_P99'")


def downgrade() -> None:
    # PostgreSQL does not support DROP VALUE from an enum.
    # Remove rows that used the new values as a best-effort rollback.
    op.execute(
        "DELETE FROM evaluation_indicators "
        "WHERE indicator IN ('FAULT_TOLERANCE_RATIO', 'RESPONSE_TIME_P99')"
    )
