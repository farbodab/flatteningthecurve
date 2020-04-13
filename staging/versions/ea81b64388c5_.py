"""empty message

Revision ID: ea81b64388c5
Revises: 4b1176c56346
Create Date: 2020-04-12 19:59:56.139360

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ea81b64388c5'
down_revision = '4b1176c56346'
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
