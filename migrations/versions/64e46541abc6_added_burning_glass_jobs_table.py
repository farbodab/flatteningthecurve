"""added burning glass jobs table

Revision ID: 64e46541abc6
Revises: 2c07722b3443
Create Date: 2020-06-25 10:00:41.640855

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '64e46541abc6'
down_revision = '2c07722b3443'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('weeklyjobposting',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('country_code', sa.String(), nullable=True),
    sa.Column('country', sa.String(), nullable=True),
    sa.Column('geography', sa.String(), nullable=True),
    sa.Column('geography_type', sa.String(), nullable=True),
    sa.Column('geography_code', sa.String(), nullable=True),
    sa.Column('group_name', sa.String(), nullable=True),
    sa.Column('start_date', sa.DateTime(), nullable=True),
    sa.Column('end_date', sa.DateTime(), nullable=True),
    sa.Column('job_postings_count', sa.String(), nullable=True),
    sa.Column('population', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_weeklyjobposting_end_date'), 'weeklyjobposting', ['end_date'], unique=False)
    op.create_index(op.f('ix_weeklyjobposting_start_date'), 'weeklyjobposting', ['start_date'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_weeklyjobposting_start_date'), table_name='weeklyjobposting')
    op.drop_index(op.f('ix_weeklyjobposting_end_date'), table_name='weeklyjobposting')
    op.drop_table('weeklyjobposting')
    # ### end Alembic commands ###
