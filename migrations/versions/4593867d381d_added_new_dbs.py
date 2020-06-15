"""added new dbs

Revision ID: 4593867d381d
Revises: 82760f5aca75
Create Date: 2020-06-15 09:28:37.189923

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '4593867d381d'
down_revision = '82760f5aca75'
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
    op.create_table('npiinterventions_world',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('country', sa.String(), nullable=True),
    sa.Column('country_code', sa.String(), nullable=True),
    sa.Column('date', sa.DateTime(), nullable=True),
    sa.Column('c1_school_closing', sa.String(), nullable=True),
    sa.Column('c1_flag', sa.String(), nullable=True),
    sa.Column('c2_workplace_closing', sa.String(), nullable=True),
    sa.Column('c2_flag', sa.String(), nullable=True),
    sa.Column('c3_cancel_public_events', sa.String(), nullable=True),
    sa.Column('c3_flag', sa.String(), nullable=True),
    sa.Column('c4_restrictions_on_gatherings', sa.String(), nullable=True),
    sa.Column('c4_flag', sa.String(), nullable=True),
    sa.Column('c5_close_public_transport', sa.String(), nullable=True),
    sa.Column('c5_flag', sa.String(), nullable=True),
    sa.Column('c6_stay_at_home_requirements', sa.String(), nullable=True),
    sa.Column('c6_flag', sa.String(), nullable=True),
    sa.Column('c7_restrictions_on_internal_movement', sa.String(), nullable=True),
    sa.Column('c7_flag', sa.String(), nullable=True),
    sa.Column('c8_international_travel_controls', sa.String(), nullable=True),
    sa.Column('e1_income_support', sa.String(), nullable=True),
    sa.Column('e1_flag', sa.String(), nullable=True),
    sa.Column('e2_debt_contract_relief', sa.String(), nullable=True),
    sa.Column('e3_fiscal_measures', sa.String(), nullable=True),
    sa.Column('e4_international_support', sa.String(), nullable=True),
    sa.Column('h1_public_information_campaigns', sa.String(), nullable=True),
    sa.Column('h1_flag', sa.String(), nullable=True),
    sa.Column('h2_testing_policy', sa.String(), nullable=True),
    sa.Column('h3_contact_tracing', sa.String(), nullable=True),
    sa.Column('h4_emergency_investment_in_healthcare', sa.String(), nullable=True),
    sa.Column('h5_investment_in_vaccines', sa.String(), nullable=True),
    sa.Column('m1_wildcard', sa.String(), nullable=True),
    sa.Column('confirmed_cases', sa.String(), nullable=True),
    sa.Column('confirmed_deaths', sa.String(), nullable=True),
    sa.Column('stringency_index', sa.String(), nullable=True),
    sa.Column('stringency_index_for_display', sa.String(), nullable=True),
    sa.Column('stringency_legacy_index', sa.String(), nullable=True),
    sa.Column('stringency_legacy_index_for_display', sa.String(), nullable=True),
    sa.Column('government_response_index', sa.String(), nullable=True),
    sa.Column('government_response_index_for_display', sa.String(), nullable=True),
    sa.Column('containment_health_index', sa.String(), nullable=True),
    sa.Column('containment_health_index_for_display', sa.String(), nullable=True),
    sa.Column('economic_support_index', sa.String(), nullable=True),
    sa.Column('economic_support_index_for_display', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_npiinterventions_world_country'), 'npiinterventions_world', ['country'], unique=False)
    op.create_index(op.f('ix_npiinterventions_world_date'), 'npiinterventions_world', ['date'], unique=False)
    op.drop_index('ix_npiinterventions_country', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_end_date', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_intervention_summary', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_region', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_start_date', table_name='npiinterventions')
    op.drop_index('ix_npiinterventions_subregion', table_name='npiinterventions')
    op.drop_table('npiinterventions')
    op.drop_index('ix_governmentresponse_country', table_name='governmentresponse')
    op.drop_index('ix_governmentresponse_date', table_name='governmentresponse')
    op.drop_table('governmentresponse')
    op.drop_column('members', 'linkedin')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('members', sa.Column('linkedin', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.create_table('governmentresponse',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('country', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('country_code', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('s1_school_closing', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s1_is_general', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s1_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s2_workplace_closing', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s2_is_general', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s2_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s3_cancel_public_events', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s3_is_general', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s3_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s4_close_public_transport', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s4_is_general', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s4_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s5_public_information_campaigns', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s5_is_general', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s5_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s6_restrictions_on_internal_movement', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s6_is_general', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s6_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s7_international_travel_controls', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s7_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s8_fiscal_measures', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('s8_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s9_monetary_measures', postgresql.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=True),
    sa.Column('s9_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s10_emergency_investment_in_healthcare', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('s10_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s11_investement_in_vaccines', sa.BIGINT(), autoincrement=False, nullable=True),
    sa.Column('s11_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s12_testing_framework', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s12_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('s13_contact_tracing', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('s13_notes', sa.VARCHAR(), autoincrement=False, nullable=True),
    sa.Column('confirmed_cases', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('confirmed_deaths', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('stringency_index', postgresql.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=True),
    sa.Column('stringency_index_for_display', postgresql.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='governmentresponse_pkey')
    )
    op.create_index('ix_governmentresponse_date', 'governmentresponse', ['date'], unique=False)
    op.create_index('ix_governmentresponse_country', 'governmentresponse', ['country'], unique=False)
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
    op.drop_index(op.f('ix_npiinterventions_world_date'), table_name='npiinterventions_world')
    op.drop_index(op.f('ix_npiinterventions_world_country'), table_name='npiinterventions_world')
    op.drop_table('npiinterventions_world')
    op.drop_index(op.f('ix_npiintervention_subregion'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_start_date'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_region'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_intervention_summary'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_end_date'), table_name='npiintervention')
    op.drop_index(op.f('ix_npiintervention_country'), table_name='npiintervention')
    op.drop_table('npiintervention')
    # ### end Alembic commands ###
