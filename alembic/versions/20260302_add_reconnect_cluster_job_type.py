"""add_reconnect_cluster_job_type

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-03-02 00:00:00.000000+00:00

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'RECONNECT_CLUSTER'")


def downgrade() -> None:
    # PostgreSQL does not support removing enum values.
    pass
