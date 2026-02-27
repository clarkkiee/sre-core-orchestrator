"""create_deployments_table

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-02-26 00:00:00.000000+00:00

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add DEPLOY_APPLICATION to existing jobtype enum
    op.execute("ALTER TYPE jobtype ADD VALUE IF NOT EXISTS 'DEPLOY_APPLICATION'")

    # 2. Create deploymentstatus enum
    deploymentstatus = postgresql.ENUM(
        "PENDING",
        "CLONING",
        "VALIDATING",
        "DEPLOYING",
        "COMPLETED",
        "FAILED",
        name="deploymentstatus",
        create_type=False,
    )
    deploymentstatus.create(op.get_bind(), checkfirst=True)

    # 3. Create deploystrategy enum
    deploystrategy = postgresql.ENUM(
        "raw",
        "helm",
        "skaffold",
        "kustomize",
        name="deploystrategy",
        create_type=False,
    )
    deploystrategy.create(op.get_bind(), checkfirst=True)

    # 4. Create deployments table
    op.create_table(
        "deployments",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("cluster_id", sa.UUID(), nullable=False),
        sa.Column(
            "repo_url",
            sa.String(500),
            nullable=False,
            comment="GitHub repository URL to deploy from",
        ),
        sa.Column(
            "branch",
            sa.String(255),
            nullable=False,
            comment="Git branch to checkout",
        ),
        sa.Column(
            "strategy",
            postgresql.ENUM(
                "raw",
                "helm",
                "skaffold",
                "kustomize",
                name="deploystrategy",
                create_type=False,
            ),
            nullable=False,
            comment="Deployment strategy: raw, helm, skaffold, kustomize",
        ),
        sa.Column(
            "namespace",
            sa.String(255),
            nullable=False,
            comment="Kubernetes namespace to deploy into",
        ),
        sa.Column(
            "platform_config",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=True,
            comment="Parsed .platform.yaml content",
        ),
        sa.Column(
            "status",
            postgresql.ENUM(
                "PENDING",
                "CLONING",
                "VALIDATING",
                "DEPLOYING",
                "COMPLETED",
                "FAILED",
                name="deploymentstatus",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column(
            "status_message",
            sa.Text(),
            nullable=True,
            comment="Additional information about the current deployment status",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["cluster_id"], ["clusters.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_deployments_tenant_id", "deployments", ["tenant_id"])
    op.create_index("ix_deployments_cluster_id", "deployments", ["cluster_id"])
    op.create_index("ix_deployments_status", "deployments", ["status"])
    op.create_index("ix_deployments_strategy", "deployments", ["strategy"])

    # 5. Add deployment_id column to jobs table
    op.add_column(
        "jobs",
        sa.Column(
            "deployment_id",
            sa.UUID(),
            nullable=True,
            comment="Associated deployment, if this job is a deploy task",
        ),
    )
    op.create_foreign_key(
        "fk_jobs_deployment_id",
        "jobs",
        "deployments",
        ["deployment_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_jobs_deployment_id", "jobs", ["deployment_id"])


def downgrade() -> None:
    op.drop_index("ix_jobs_deployment_id", table_name="jobs")
    op.drop_constraint("fk_jobs_deployment_id", "jobs", type_="foreignkey")
    op.drop_column("jobs", "deployment_id")

    op.drop_index("ix_deployments_strategy", table_name="deployments")
    op.drop_index("ix_deployments_status", table_name="deployments")
    op.drop_index("ix_deployments_cluster_id", table_name="deployments")
    op.drop_index("ix_deployments_tenant_id", table_name="deployments")
    op.drop_table("deployments")

    # Note: PostgreSQL does not support removing enum values;
    # enums will remain after downgrade
