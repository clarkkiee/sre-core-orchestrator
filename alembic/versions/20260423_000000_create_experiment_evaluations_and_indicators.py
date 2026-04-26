"""create_experiment_evaluations_and_evaluation_indicators

Revision ID: b1c2d3e4f5a6
Revises: a92167ab19dd
Create Date: 2026-04-23 00:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b1c2d3e4f5a6'
down_revision: Union[str, None] = 'a92167ab19dd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'experiment_evaluations',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('experiment_id', sa.UUID(), nullable=False),
        sa.Column('baseline_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('baseline_end', sa.DateTime(timezone=True), nullable=False),
        sa.Column('fault_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('fault_end', sa.DateTime(timezone=True), nullable=False),
        sa.Column('recovery_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('recovery_end', sa.DateTime(timezone=True), nullable=False),
        sa.Column('litmus_probe_percentage', sa.Float(), nullable=True),
        sa.Column(
            'status',
            sa.Enum('SUCCESS', 'PARTIAL', 'FAILED', name='evaluationstatus'),
            nullable=False,
            server_default='SUCCESS',
        ),
        sa.Column('status_message', sa.Text(), nullable=True),
        sa.Column('evaluator_version', sa.String(32), nullable=False, server_default='v1'),
        sa.Column(
            'evaluated_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['experiment_id'], ['chaos_experiments.id'], ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('experiment_id'),
    )
    op.create_index(
        'ix_experiment_evaluations_experiment_id',
        'experiment_evaluations',
        ['experiment_id'],
        unique=False,
    )

    # metricphase enum was created by migration a92167ab19dd — reuse, do not recreate.
    metricphase = postgresql.ENUM(
        'BASELINE', 'FAULT', 'RECOVERY',
        name='metricphase',
        create_type=False,
    )
    op.create_table(
        'evaluation_indicators',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('evaluation_id', sa.UUID(), nullable=False),
        sa.Column(
            'indicator',
            sa.Enum(
                'SYSTEM_AVAILABILITY',
                'MEAN_DOWN_TIME',
                'FAILURE_RATE',
                'MEAN_RECOVERY_TIME',
                name='isoindicator',
            ),
            nullable=False,
        ),
        sa.Column('phase', metricphase, nullable=True),
        sa.Column(
            'scope',
            sa.Enum('TARGET', 'PEER', 'NAMESPACE_WIDE', name='measurementscope'),
            nullable=False,
        ),
        sa.Column('value', sa.Float(), nullable=False),
        sa.Column('sample_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('episode_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('formula_version', sa.String(64), nullable=False),
        sa.Column(
            'extra',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            'computed_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['evaluation_id'], ['experiment_evaluations.id'], ondelete='CASCADE'
        ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'evaluation_id',
            'indicator',
            'phase',
            'scope',
            'formula_version',
            name='uq_evaluation_indicator_dimensions',
        ),
    )
    op.create_index(
        'ix_evaluation_indicators_evaluation_id',
        'evaluation_indicators',
        ['evaluation_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_evaluation_indicators_evaluation_id', table_name='evaluation_indicators')
    op.drop_table('evaluation_indicators')
    op.drop_index('ix_experiment_evaluations_experiment_id', table_name='experiment_evaluations')
    op.drop_table('experiment_evaluations')

    # Drop enums created by this migration only.
    # metricphase is NOT dropped — it predates this migration (created by a92167ab19dd).
    sa.Enum(name='measurementscope').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='isoindicator').drop(op.get_bind(), checkfirst=True)
    sa.Enum(name='evaluationstatus').drop(op.get_bind(), checkfirst=True)
