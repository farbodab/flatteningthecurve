from flask import Blueprint
import click


bp = Blueprint('pipeline', __name__,cli_group='pipeline')

from app.data_in import routes as data_in
from app.data_process import routes as data_process
from app.data_transform import routes as data_transform
from app.data_export import routes as data_export

@bp.cli.command('gov')
@click.pass_context
def gov_ontario(ctx):
    ctx.forward(data_in.get_public_ontario_gov_conposcovidloc)
    ctx.forward(data_process.process_public_ontario_gov_conposcovidloc)
    ctx.forward(data_transform.transform_public_cases_ontario_confirmed_positive_cases)
    ctx.forward(data_transform.transform_public_cases_ontario_cases_seven_day_rolling_average)
    ctx.forward(data_transform.transform_public_cases_ontario_phu_weekly_new_cases)
    ctx.forward(data_transform.transform_public_capacity_ontario_testing_24_hours)
    ctx.forward(data_transform.transform_public_summary_ontario)
    # ctx.forward(data_export.export_public_cases_ontario_confirmed_positive_cases)
    ctx.forward(data_export.export_public_summary_ontario)
    ctx.forward(data_export.export_public_cases_ontario_phu_weekly_new_cases)
    ctx.forward(data_export.export_public_capacity_ontario_testing_24_hours)
    ctx.forward(data_process.process_restricted_moh_iphis)
    ctx.forward(data_transform.transform_confidential_moh_iphis)
    ctx.forward(data_export.export_confidential_moh_iphis)

@bp.cli.command('ccso')
@click.pass_context
def ccso(ctx):
    ctx.forward(data_process.process_restricted_ccso_ccis)
    ctx.forward(data_transform.transform_public_capacity_ontario_phu_icu_capacity)
    ctx.forward(data_transform.transform_public_capacity_ontario_phu_icu_capacity_timeseries)
    ctx.forward(data_export.export_public_capacity_ontario_phu_icu_capacity)

@bp.cli.command('rt')
@click.pass_context
def rt(ctx):
    ctx.forward(data_in.get_public_open_data_working_group_cases)
    ctx.forward(data_process.process_public_open_data_working_group_cases)
    ctx.forward(data_transform.transform_public_cases_canada_confirmed_positive_cases)
    ctx.forward(data_transform.transform_public_rt_canada_bettencourt_and_ribeiro_approach)
    ctx.forward(data_export.export_public_rt_canada_bettencourt_and_ribeiro_approach)
