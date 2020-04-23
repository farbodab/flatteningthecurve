"""added new tables

Revision ID: 26c1b7e78e8d
Revises: 38b498e051db
Create Date: 2020-04-23 14:56:44.942465

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '26c1b7e78e8d'
down_revision = '38b498e051db'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('viz', sa.Column('column', sa.Integer(), nullable=True))
    op.add_column('viz', sa.Column('html', sa.String(), nullable=True))
    op.add_column('viz', sa.Column('order', sa.Integer(), nullable=True))
    op.add_column('viz', sa.Column('page', sa.String(), nullable=True))
    op.add_column('viz', sa.Column('row', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_viz_page'), 'viz', ['page'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_viz_page'), table_name='viz')
    op.drop_column('viz', 'row')
    op.drop_column('viz', 'page')
    op.drop_column('viz', 'order')
    op.drop_column('viz', 'html')
    op.drop_column('viz', 'column')
    # ### end Alembic commands ###