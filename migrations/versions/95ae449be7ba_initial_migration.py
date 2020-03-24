"""Initial migration

Revision ID: 95ae449be7ba
Revises: 
Create Date: 2020-03-23 12:43:01.096206

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '95ae449be7ba'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('covidDate',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('province', sa.String(), nullable=True),
    sa.Column('date', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_covidDate_date'), 'covidDate', ['date'], unique=False)
    op.create_index(op.f('ix_covidDate_province'), 'covidDate', ['province'], unique=False)
    op.create_table('covids',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=True),
    sa.Column('province', sa.String(), nullable=True),
    sa.Column('count', sa.Integer(), nullable=True),
    sa.Column('hundred', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_covids_date'), 'covids', ['date'], unique=False)
    op.create_index(op.f('ix_covids_province'), 'covids', ['province'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_covids_province'), table_name='covids')
    op.drop_index(op.f('ix_covids_date'), table_name='covids')
    op.drop_table('covids')
    op.drop_index(op.f('ix_covidDate_province'), table_name='covidDate')
    op.drop_index(op.f('ix_covidDate_date'), table_name='covidDate')
    op.drop_table('covidDate')
    # ### end Alembic commands ###