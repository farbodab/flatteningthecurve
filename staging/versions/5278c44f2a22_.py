"""empty message

Revision ID: 5278c44f2a22
Revises: 585f23a463e5
Create Date: 2020-06-14 23:50:07.724418

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '5278c44f2a22'
down_revision = '585f23a463e5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('npiintervention',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=True),
    sa.Column('end_date', sa.DateTime(), nullable=True),
    sa.Column('country', sa.String(), nullable=True),
    sa.Column('region', sa.String(), nullable=True),
    sa.Column('subregion', sa.String(), nullable=True),
    sa.Column('intervention_summary', sa.String(), nullable=True),
    sa.Column('intervention_category', sa.String(), nullable=True),
    sa.Column('target_population_category', sa.String(), nullable=True),
    sa.Column('enforcement_category', sa.String(), nullable=True),
    sa.Column('oxford_government_response_category', sa.String(), nullable=True),
    sa.Column('oxford_closure_code', sa.String(), nullable=True),
    sa.Column('oxford_public_info_code', sa.String(), nullable=True),
    sa.Column('oxford_travel_code', sa.String(), nullable=True),
    sa.Column('oxford_geographic_target_code', sa.String(), nullable=True),
    sa.Column('oxford_fiscal_measure_cad', sa.String(), nullable=True),
    sa.Column('oxford_testing_code', sa.String(), nullable=True),
    sa.Column('oxford_tracing_code', sa.String(), nullable=True),
    sa.Column('oxford_restriction_code', sa.String(), nullable=True),
    sa.Column('oxford_income_amount', sa.String(), nullable=True),
    sa.Column('oxford_debt_relief_code', sa.String(), nullable=True),
    sa.Column('source_url', sa.String(), nullable=True),
    sa.Column('source_organization', sa.String(), nullable=True),
    sa.Column('source_organization_2', sa.String(), nullable=True),
    sa.Column('source_category', sa.String(), nullable=True),
    sa.Column('source_title', sa.String(), nullable=True),
    sa.Column('source_full_text', sa.String(), nullable=True),
    sa.Column('note', sa.String(), nullable=True),
    sa.Column('end_source_url', sa.String(), nullable=True),
    sa.Column('end_source_organization', sa.String(), nullable=True),
    sa.Column('end_source_organization_2', sa.String(), nullable=True),
    sa.Column('end_source_category', sa.String(), nullable=True),
    sa.Column('end_source_title', sa.String(), nullable=True),
    sa.Column('end_source_full_text', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_npiintervention_country'), 'npiintervention', ['country'], unique=False)
    op.create_index(op.f('ix_npiintervention_end_date'), 'npiintervention', ['end_date'], unique=False)
    op.create_index(op.f('ix_npiintervention_intervention_summary'), 'npiintervention', ['intervention_summary'], unique=False)
    op.create_index(op.f('ix_npiintervention_region'), 'npiintervention', ['region'], unique=False)
    op.create_index(op.f('ix_npiintervention_start_date'), 'npiintervention', ['start_date'], unique=False)
    op.create_index(op.f('ix_npiintervention_subregion'), 'npiintervention', ['subregion'], unique=False)
    op.drop_index('ix_npiinterventions_country', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_end_date', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_intervention_summary', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_region', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_start_date', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_subregion', table_name='npiinterventions')
    op.drop_table('npiinterventions')
    op.drop_column('members', 'linkedin')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('members', sa.Column('linkedin', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.create_table('npiinterventions',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('start_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('end_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('country', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('region', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('intervention_summary', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('intervention_category', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('target_population_category', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('enforcement_category', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_government_response_category', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_closure_code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_public_info_code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_travel_code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_geographic_target_code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_fiscal_measure_cad', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_monetary_measure', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('source_url', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('source_organization', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('source_organization_two', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('source_category', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('source_title', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('source_full_text', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_testing_code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('oxford_tracing_code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('subregion', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='npiinterventions_pkey')
    )
    op.create_index('ix_npiinterventions_subregion', 'npiinterventions', ['subregion'], unique=False)
    op.create_index('ix_npiinterventions_start_date', 'npiinterventions', ['start_date'], unique=False)
    op.create_index('ix_npiinterventions_region', 'npiinterventions', ['region'], unique=False)
    op.create_index('ix_npiinterventions_intervention_summary', 'npiinterventions', ['intervention_summary'], unique=False)
    op.create_index('ix_npiinterventions_end_date', 'npiinterventions', ['end_date'], unique=False)
    op.create_index('ix_npiinterventions_country', 'npiinterventions', ['country'], unique=False)
    op.drop_index(op.f('ix_npiintervention_subregion'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_start_date'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_region'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_intervention_summary'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_end_date'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_country'), table_name='npiintervention')
    op.drop_table('npiintervention')
    # ### end Alembic commands ###