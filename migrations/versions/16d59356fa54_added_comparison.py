"""added comparison

Revision ID: 16d59356fa54
Revises: faaf679b71ce
Create Date: 2020-03-23 15:04:32.298712

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '16d59356fa54'
down_revision = 'faaf679b71ce'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('Comparison',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=True),
    sa.Column('province', sa.String(), nullable=True),
    sa.Column('count', sa.Integer(), nullable=True),
    sa.Column('hundred', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_Comparison_date'), 'Comparison', ['date'], unique=False)
    op.create_index(op.f('ix_Comparison_province'), 'Comparison', ['province'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_Comparison_province'), table_name='Comparison')
    op.drop_index(op.f('ix_Comparison_date'), table_name='Comparison')
    op.drop_table('Comparison')
    # ### end Alembic commands ###