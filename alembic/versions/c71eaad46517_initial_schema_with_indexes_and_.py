"""initial_schema_with_indexes_and_idempotency

Revision ID: c71eaad46517
Revises: 
Create Date: 2026-10-30 09:26:51.795645

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c71eaad46517'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Accounts Table
    op.create_table(
        'accounts',
        sa.Column('account_id', sa.String(length=50), nullable=False),
        sa.Column('owner_name', sa.String(length=100), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('account_id')
    )
    op.create_index('ix_accounts_account_id', 'accounts', ['account_id'], unique=False)

    # 2. Transactions Table (with event_id idempotency & indexes)
    op.create_table(
        'transactions',
        sa.Column('transaction_id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('event_id', sa.String(length=64), nullable=False),
        sa.Column('account_id', sa.String(length=50), nullable=False),
        sa.Column('amount', sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column('location', sa.String(length=100), nullable=True),
        sa.Column('merchant', sa.String(length=100), nullable=True),
        sa.Column('status', sa.String(length=20), server_default='normal', nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['account_id'], ['accounts.account_id'], ),
        sa.PrimaryKeyConstraint('transaction_id'),
    )
    op.create_index('ix_transactions_event_id', 'transactions', ['event_id'], unique=True)
    op.create_index('ix_transactions_account_id', 'transactions', ['account_id'], unique=False)
    op.create_index('ix_transactions_status', 'transactions', ['status'], unique=False)
    op.create_index('ix_transactions_timestamp', 'transactions', ['timestamp'], unique=False)
    op.create_index('ix_transactions_account_timestamp', 'transactions', ['account_id', 'timestamp'], unique=False)

    # 3. Anomalies Table
    op.create_table(
        'anomalies',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.Column('risk_score', sa.Numeric(precision=4, scale=2), nullable=True),
        sa.Column('reason', sa.String(length=255), nullable=True),
        sa.Column('detected_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['transaction_id'], ['transactions.transaction_id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_anomalies_transaction_id', 'anomalies', ['transaction_id'], unique=False)
    op.create_index('ix_anomalies_risk_score', 'anomalies', ['risk_score'], unique=False)
    op.create_index('ix_anomalies_detected_at', 'anomalies', ['detected_at'], unique=False)

    # 4. ML Score Tables
    op.create_table(
        'ml_scores',
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.Column('anomaly_score', sa.Numeric(), nullable=True),
        sa.Column('ml_anomaly', sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint('transaction_id')
    )

    op.create_table(
        'ml_scores_lr',
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.Column('lr_prediction', sa.Integer(), nullable=True),
        sa.Column('lr_probability', sa.Numeric(), nullable=True),
        sa.PrimaryKeyConstraint('transaction_id')
    )

    op.create_table(
        'model_comparison',
        sa.Column('transaction_id', sa.Integer(), nullable=False),
        sa.Column('account_id', sa.String(length=50), nullable=True),
        sa.Column('amount', sa.Numeric(precision=12, scale=2), nullable=True),
        sa.Column('rule_flag', sa.Integer(), nullable=True),
        sa.Column('ml_anomaly', sa.Integer(), nullable=True),
        sa.Column('anomaly_score', sa.Numeric(), nullable=True),
        sa.Column('lr_prediction', sa.Integer(), nullable=True),
        sa.Column('lr_probability', sa.Numeric(), nullable=True),
        sa.PrimaryKeyConstraint('transaction_id')
    )


def downgrade() -> None:
    op.drop_table('model_comparison')
    op.drop_table('ml_scores_lr')
    op.drop_table('ml_scores')
    op.drop_index('ix_anomalies_detected_at', table_name='anomalies')
    op.drop_index('ix_anomalies_risk_score', table_name='anomalies')
    op.drop_index('ix_anomalies_transaction_id', table_name='anomalies')
    op.drop_table('anomalies')
    op.drop_index('ix_transactions_account_timestamp', table_name='transactions')
    op.drop_index('ix_transactions_timestamp', table_name='transactions')
    op.drop_index('ix_transactions_status', table_name='transactions')
    op.drop_index('ix_transactions_account_id', table_name='transactions')
    op.drop_index('ix_transactions_event_id', table_name='transactions')
    op.drop_table('transactions')
    op.drop_index('ix_accounts_account_id', table_name='accounts')
    op.drop_table('accounts')
