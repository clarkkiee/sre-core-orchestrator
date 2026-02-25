"""add_kubeconfig_column_to_clusters

Revision ID: a1b2c3d4e5f6
Revises: f86b52b41e91
Create Date: 2026-02-25 00:00:00.000000+00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "f86b52b41e91"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "clusters",
        sa.Column(
            "kubeconfig",
            sa.Text(),
            nullable=True,
            comment="Kubeconfig YAML content stored directly in DB",
        ),
    )


def downgrade() -> None:
    op.drop_column("clusters", "kubeconfig")
