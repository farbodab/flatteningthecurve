"""fixed heads

Revision ID: 2c07722b3443
Revises: 5c1205c1f53e
Create Date: 2020-06-15 09:51:03.027171

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2c07722b3443'
down_revision = '5c1205c1f53e'
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
