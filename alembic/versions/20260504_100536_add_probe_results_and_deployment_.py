"""add_probe_results_and_deployment_thresholds

Revision ID: df0ab4a326b7
Revises: dffb450c3a10
Create Date: 2026-05-04 10:05:36.371208+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'df0ab4a326b7'
down_revision: Union[str, None] = 'dffb450c3a10'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.create_table(
        'probe_results',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('evaluation_id', sa.UUID(), nullable=False),
        sa.Column('probe_name', sa.String(120), nullable=False),
        sa.Column(
            'probe_type',
            sa.Enum('HTTP', 'CMD', 'PROM', 'K8S', name='probetype'),
            nullable=False,
        ),
        sa.Column(
            'probe_mode',
            sa.Enum(
                'SOT', 'EOT', 'EDGE', 'CONTINUOUS', 'ON_CHAOS',
                name='probemode',
            ),
            nullable=False,
        ),
        sa.Column(
            'verdict',
            sa.Enum('PASSED', 'FAILED', 'NA', name='probeverdict'),
            nullable=False,
        ),
        sa.Column('success_percentage', sa.Float(), nullable=True),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('failure_step', sa.Text(), nullable=True),
        sa.Column('error_output', sa.Text(), nullable=True),
        # linked_indicator deliberately omitted here — added in Step 2 below.
        sa.Column(
            'spec',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            'extra',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            'recorded_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['evaluation_id'], ['experiment_evaluations.id'], ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'evaluation_id', 'probe_name',
            name='uq_probe_results_evaluation_probe',
        ),
    )

    # Step 2: Add linked_indicator column referencing the EXISTING isoindicator enum.
    # Raw SQL bypasses any SQLAlchemy enum auto-creation logic.
    op.execute(
        "ALTER TABLE probe_results ADD COLUMN linked_indicator isoindicator NULL"
    )

    op.create_index(
        'ix_probe_results_evaluation_id',
        'probe_results',
        ['evaluation_id'],
        unique=False,
    )
    op.create_index(
        'ix_probe_results_type_verdict',
        'probe_results',
        ['probe_type', 'verdict'],
        unique=False,
    )

    # Per-deployment threshold overrides
    op.add_column(
        'deployments',
        sa.Column(
            'probe_thresholds',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
            comment=(
                'Per-deployment SLO threshold overrides for Litmus probes. '
                'Falls back to global PROBE_DEFAULT_* settings.'
            ),
        ),
    )

def downgrade() -> None:
    op.drop_column('deployments', 'probe_thresholds')

    op.drop_index('ix_probe_results_type_verdict', table_name='probe_results')
    op.drop_index('ix_probe_results_evaluation_id', table_name='probe_results')
    op.drop_table('probe_results')

    # Drop enums created by THIS migration only.
    # isoindicator is NOT dropped — it predates this migration.
    sa.Enum(name='probeverdict').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='probemode').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='probetype').drop(op.get_bind(), checkfirst=True)