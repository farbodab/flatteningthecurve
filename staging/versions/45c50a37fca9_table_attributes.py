"""table attributes

Revision ID: 45c50a37fca9
Revises: a7535fc05877
Create Date: 2020-06-10 19:05:45.561381

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '45c50a37fca9'
down_revision = 'a7535fc05877'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    # op.drop_index('ix_healthregions_eng_name', table_name='healthregions')
    # op.drop_index('ix_healthregions_fr_name', table_name='healthregions')
    # op.drop_table('healthregions')
    # op.drop_index('ix_healthregionsdata_date', table_name='healthregionsdata')
    # op.drop_table('healthregionsdata')
    op.add_column('ideamodel', sa.Column('date_retrieved', sa.DateTime(), nullable=True))
    op.create_index(op.f('ix_ideamodel_date_retrieved'), 'ideamodel', ['date_retrieved'], unique=False)
    op.add_column('predictivemodel', sa.Column('date_retrieved', sa.DateTime(), nullable=True))
    op.create_index(op.f('ix_predictivemodel_date_retrieved'), 'predictivemodel', ['date_retrieved'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_predictivemodel_date_retrieved'), table_name='predictivemodel')
    op.drop_column('predictivemodel', 'date_retrieved')
    op.drop_index(op.f('ix_ideamodel_date_retrieved'), table_name='ideamodel')
    op.drop_column('ideamodel', 'date_retrieved')
    # op.create_table('healthregionsdata',
    # sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    # sa.Column('region_id', sa.INTEGER(), autoincrement=False, nullable=True),
    # sa.Column('cases_cumulative', sa.INTEGER(), autoincrement=False, nullable=True),
    # sa.Column('deaths_cumulative', sa.INTEGER(), autoincrement=False, nullable=True),
    # sa.Column('recovered_cumulative', sa.INTEGER(), autoincrement=False, nullable=True),
    # sa.Column('tests_cumulative', sa.INTEGER(), autoincrement=False, nullable=True),
    # sa.Column('date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    # sa.ForeignKeyConstraint(['region_id'], ['healthregions.id'], name='healthregionsdata_region_id_fkey'),
    # sa.PrimaryKeyConstraint('id', name='healthregionsdata_pkey')
    # )
    # op.create_index('ix_healthregionsdata_date', 'healthregionsdata', ['date'], unique=False)
    # op.create_table('healthregions',
    # sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    # sa.Column('province', sa.VARCHAR(), autoincrement=False, nullable=True),
    # sa.Column('eng_name', sa.VARCHAR(), autoincrement=False, nullable=True),
    # sa.Column('fr_name', sa.VARCHAR(), autoincrement=False, nullable=True),
    # sa.Column('population', sa.INTEGER(), autoincrement=False, nullable=True),
    # sa.PrimaryKeyConstraint('id', name='healthregions_pkey')
    # )
    # op.create_index('ix_healthregions_fr_name', 'healthregions', ['fr_name'], unique=False)
    # op.create_index('ix_healthregions_eng_name', 'healthregions', ['eng_name'], unique=False)
    # ### end Alembic commands ###
