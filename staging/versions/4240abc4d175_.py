"""empty message

Revision ID: 4240abc4d175
Revises: 585f23a463e5
Create Date: 2020-06-14 16:39:20.656463

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4240abc4d175'
down_revision = '585f23a463e5'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('members', sa.Column('linkedin', sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('members', 'linkedin')
    # ### end Alembic commands ###
