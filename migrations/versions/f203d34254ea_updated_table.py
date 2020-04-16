"""updated table

Revision ID: f203d34254ea
Revises: 10388298852c
Create Date: 2020-04-12 20:25:35.706980

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f203d34254ea'
down_revision = '10388298852c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('npiinterventions', sa.Column('oxford_testing_code', sa.String(), nullable=True))
    op.add_column('npiinterventions', sa.Column('oxford_tracing_code', sa.String(), nullable=True))
    op.add_column('npiinterventions', sa.Column('subregion', sa.String(), nullable=True))
    op.create_index(op.f('ix_npiinterventions_subregion'), 'npiinterventions', ['subregion'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_npiinterventions_subregion'), table_name='npiinterventions')
    op.drop_column('npiinterventions', 'subregion')
    op.drop_column('npiinterventions', 'oxford_tracing_code')
    op.drop_column('npiinterventions', 'oxford_testing_code')
    # ### end Alembic commands ###