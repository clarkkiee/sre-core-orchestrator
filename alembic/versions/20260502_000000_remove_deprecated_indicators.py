"""remove_deprecated_indicators

Removes evaluation_indicator rows for FAILURE_RATE (redundant with
SYSTEM_AVAILABILITY — same probe_success signal) and
MEAN_FAULT_NOTIFICATION_TIME (unimplementable without a separate alerting
layer; always identical to MTTF in this setup).

The isoindicator PostgreSQL enum type retains these values to avoid a
complex type-rebuild, but no new rows will be created for them.

Revision ID: d63d9e2ec25c
Revises: 748624ce3631
Create Date: 2026-05-02 00:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'd63d9e2ec25c'
down_revision: Union[str, None] = '748624ce3631'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DEPRECATED = ('FAILURE_RATE', 'MEAN_FAULT_NOTIFICATION_TIME')


def upgrade() -> None:
    op.execute(
        "DELETE FROM evaluation_indicators "
        f"WHERE indicator IN {_DEPRECATED!r}"
    )


def downgrade() -> None:
    # Rows cannot be restored after deletion.
    pass
