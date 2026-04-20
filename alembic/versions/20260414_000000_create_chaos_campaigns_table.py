"""create_chaos_campaigns_table

Revision ID: a1b2c3d4e5f6
Revises: 86e8664df672
Create Date: 2026-04-14 00:00:00.000000+00:00

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f7a8b9c0d1e2"
down_revision: Union[str, None] = "86e8664df672"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add RUN_CHAOS_CAMPAIGN to the jobtype enum
    op.execute("COMMIT")
    op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'RUN_CHAOS_CAMPAIGN'")
    op.execute("BEGIN")

    # Create campaignstatus enum 
    campaignstatus = postgresql.ENUM(
        "PENDING",
        "DISCOVERING",
        "RUNNING",
        "STOPPED",
        "COMPLETED",
        "FAILED",
        name="deploymentstatus",
        create_type=False,
    )
    campaignstatus.create(op.get_bind(), checkfirst=True)

    # Create chaos_campaigns table
    op.create_table(
        "chaos_campaigns",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("cluster_id", sa.UUID(), nullable=False),
        sa.Column("deployment_id", sa.UUID(), nullable=False),
        sa.Column("target_namespace", sa.String(253), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "DISCOVERING",
                "RUNNING",
                "COMPLETED",
                "FAILED",
                "STOPPED",
                name="campaignstatus",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column("status_message", sa.Text(), nullable=True),
        sa.Column("discovered_services", sa.JSON(), nullable=True),
        sa.Column("total_experiments", sa.Integer(), nullable=False, default=0),
        sa.Column("completed_experiments", sa.Integer(), nullable=False, default=0),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"], ["users.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["cluster_id"], ["clusters.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["deployment_id"], ["deployments.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_chaos_campaigns_tenant_id", "chaos_campaigns", ["tenant_id"]
    )
    op.create_index(
        "ix_chaos_campaigns_cluster_id", "chaos_campaigns", ["cluster_id"]
    )

    # Add campaign_id FK to chaos_experiments
    op.add_column(
        "chaos_experiments",
        sa.Column("campaign_id", sa.UUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_chaos_experiments_campaign_id",
        "chaos_experiments",
        "chaos_campaigns",
        ["campaign_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_chaos_experiments_campaign_id",
        "chaos_experiments",
        ["campaign_id"],
    )

    # Add campaign_id FK to jobs
    op.add_column(
        "jobs",
        sa.Column("campaign_id", sa.UUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_jobs_campaign_id",
        "jobs",
        "chaos_campaigns",
        ["campaign_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_jobs_campaign_id", "jobs", ["campaign_id"])


def downgrade() -> None:
    # Remove campaign_id from jobs
    op.drop_index("ix_jobs_campaign_id", table_name="jobs")
    op.drop_constraint("fk_jobs_campaign_id", "jobs", type_="foreignkey")
    op.drop_column("jobs", "campaign_id")

    # Remove campaign_id from chaos_experiments
    op.drop_index(
        "ix_chaos_experiments_campaign_id", table_name="chaos_experiments"
    )
    op.drop_constraint(
        "fk_chaos_experiments_campaign_id",
        "chaos_experiments",
        type_="foreignkey",
    )
    op.drop_column("chaos_experiments", "campaign_id")

    # Drop chaos_campaigns table
    op.drop_index("ix_chaos_campaigns_cluster_id", table_name="chaos_campaigns")
    op.drop_index("ix_chaos_campaigns_tenant_id", table_name="chaos_campaigns")
    op.drop_table("chaos_campaigns")

    # Drop campaignstatus enum
    sa.Enum(name="campaignstatus").drop(op.get_bind(), checkfirst=True)
