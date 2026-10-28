"""add_reminder_sent_to_bookings

Revision ID: c1a84f329d01
Revises: 558171f3382f
Create Date: 2026-10-28 20:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c1a84f329d01'
down_revision: Union[str, None] = '558171f3382f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Добавляем колонку reminder_sent в таблицу bookings
    op.add_column(
        'bookings',
        sa.Column('reminder_sent', sa.Boolean(), server_default='false', nullable=False)
    )
    op.create_index(op.f('ix_bookings_reminder_sent'), 'bookings', ['reminder_sent'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_bookings_reminder_sent'), table_name='bookings')
    op.drop_column('bookings', 'reminder_sent')
