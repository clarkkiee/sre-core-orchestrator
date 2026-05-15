"""drop_legacy_indicators

Revision ID: 5ebab22fc33a
Revises: f1c2a8d4e9b0
Create Date: 2026-05-14 03:23:37.999754+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '5ebab22fc33a'
down_revision: Union[str, None] = 'f1c2a8d4e9b0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # 1. Clean up rows that reference the deprecated indicators.
    op.execute(
        "DELETE FROM evaluation_indicators "
        "WHERE indicator IN ('MEAN_DOWN_TIME', 'MEAN_TIME_TO_FAILURE', 'FAULT_TOLERANCE_RATIO')"
    )
    # probe_results.linked_indicator is nullable — null it out instead of deleting probe rows.
    op.execute(
        "UPDATE probe_results SET linked_indicator = NULL "
        "WHERE linked_indicator IN ('MEAN_DOWN_TIME', 'MEAN_TIME_TO_FAILURE', 'FAULT_TOLERANCE_RATIO')"
    )

    # 2. Recreate the isoindicator enum without the dropped values.
    op.execute("ALTER TYPE isoindicator RENAME TO isoindicator_old")
    op.execute(
        "CREATE TYPE isoindicator AS ENUM ("
        "'SYSTEM_AVAILABILITY', 'MEAN_RECOVERY_TIME', "
        "'RESPONSE_TIME_P95', 'RESPONSE_TIME_P99', 'ERROR_RATE', "
        "'CPU_UTILIZATION', 'MEMORY_UTILIZATION', "
        "'SUCCESS_RATE_DEGRADATION', "
        "'LATENCY_P95_DEGRADATION', 'LATENCY_P99_DEGRADATION'"
        ")"
    )

    # 3. Cast every column that uses the old enum.
    op.execute(
        "ALTER TABLE evaluation_indicators "
        "ALTER COLUMN indicator TYPE isoindicator "
        "USING indicator::text::isoindicator"
    )
    op.execute(
        "ALTER TABLE probe_results "
        "ALTER COLUMN linked_indicator TYPE isoindicator "
        "USING linked_indicator::text::isoindicator"
    )

    # 4. Drop the renamed old enum.
    op.execute("DROP TYPE isoindicator_old")


def downgrade() -> None:
    op.execute("ALTER TYPE isoindicator RENAME TO isoindicator_old")
    op.execute(
        "CREATE TYPE isoindicator AS ENUM ("
        "'SYSTEM_AVAILABILITY', 'MEAN_DOWN_TIME', 'MEAN_RECOVERY_TIME', "
        "'MEAN_TIME_TO_FAILURE', 'RESPONSE_TIME_P95', 'RESPONSE_TIME_P99', "
        "'ERROR_RATE', 'FAULT_TOLERANCE_RATIO', "
        "'CPU_UTILIZATION', 'MEMORY_UTILIZATION', "
        "'SUCCESS_RATE_DEGRADATION', "
        "'LATENCY_P95_DEGRADATION', 'LATENCY_P99_DEGRADATION'"
        ")"
    )
    op.execute(
        "ALTER TABLE evaluation_indicators "
        "ALTER COLUMN indicator TYPE isoindicator "
        "USING indicator::text::isoindicator"
    )
    op.execute(
        "ALTER TABLE probe_results "
        "ALTER COLUMN linked_indicator TYPE isoindicator "
        "USING linked_indicator::text::isoindicator"
    )
    op.execute("DROP TYPE isoindicator_old")
