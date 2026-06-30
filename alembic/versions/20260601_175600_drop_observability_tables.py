"""drop_observability_tables

Revision ID: 9565fa553b1c
Revises: 5f0599e8d622
Create Date: 2026-06-01 17:56:00.607845+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9565fa553b1c'
down_revision: Union[str, None] = '5f0599e8d622'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("reliability_reports")
    op.drop_table("metric_snapshots")
    op.drop_table("observability_sessions")
    op.execute("DROP TYPE IF EXISTS collectionphase")
    op.execute("DROP TYPE IF EXISTS sessionstatus")


def downgrade() -> None:
    raise NotImplementedError(
        "Observability subsystem removed; restore from create migration if needed."
    )