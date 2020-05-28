"""New tables

Revision ID: 31d419236710
Revises: f1f0b96dd139
Create Date: 2020-05-27 15:50:56.919999

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '31d419236710'
down_revision = 'f1f0b96dd139'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('longtermcare_nolongerinoutbreak',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=True),
    sa.Column('home', sa.String(), nullable=True),
    sa.Column('city', sa.String(), nullable=True),
    sa.Column('beds', sa.Integer(), nullable=True),
    sa.Column('resident_deaths', sa.Integer(), nullable=True),
    sa.Column('phu', sa.String(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_longtermcare_nolongerinoutbreak_date'), 'longtermcare_nolongerinoutbreak', ['date'], unique=False)
    op.create_table('longtermcare_summary',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('date', sa.DateTime(), nullable=True),
    sa.Column('report', sa.String(), nullable=True),
    sa.Column('number', sa.Integer(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_longtermcare_summary_date'), 'longtermcare_summary', ['date'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_longtermcare_summary_date'), table_name='longtermcare_summary')
    op.drop_table('longtermcare_summary')
    op.drop_index(op.f('ix_longtermcare_nolongerinoutbreak_date'), table_name='longtermcare_nolongerinoutbreak')
    op.drop_table('longtermcare_nolongerinoutbreak')
    # ### end Alembic commands ###