"""add_deleted_at_to_deployments

Revision ID: 3d5c19d28803
Revises: e9e14c7d2c2d
Create Date: 2026-03-13 05:30:07.557488+00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "3d5c19d28803"
down_revision: Union[str, None] = "e9e14c7d2c2d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add DELETING and DELETED to deploymentstatus enum
    op.execute("ALTER TYPE deploymentstatus ADD VALUE IF NOT EXISTS 'DELETING'")
    op.execute("ALTER TYPE deploymentstatus ADD VALUE IF NOT EXISTS 'DELETED'")

    # 2. Add DELETE_DEPLOYMENT to jobtype enum
    op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'DELETE_DEPLOYMENT'")

    # 3. Add deleted_at column to deployments table
    op.add_column(
        "deployments",
        sa.Column(
            "deleted_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="The deletion time of the deployment",
        ),
    )


def downgrade() -> None:
    op.drop_column("deployments", "deleted_at")
    # Note: PostgreSQL does not support removing enum values;
    # DELETING, DELETED, and DELETE_DEPLOYMENT will remain after downgrade
