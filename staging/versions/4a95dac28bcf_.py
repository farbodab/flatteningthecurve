"""empty message

Revision ID: 4a95dac28bcf
Revises: 8ff163281894
Create Date: 2020-04-10 12:32:11.843196

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4a95dac28bcf'
down_revision = '8ff163281894'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('viz', sa.Column('desktopHeight', sa.Integer(), nullable=True))
    op.add_column('viz', sa.Column('mobileHeight', sa.Integer(), nullable=True))
    op.drop_column('viz', 'height')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('viz', sa.Column('height', sa.INTEGER(), autoincrement=False, nullable=True))
    op.drop_column('viz', 'mobileHeight')
    op.drop_column('viz', 'desktopHeight')
    # ### end Alembic commands ###
