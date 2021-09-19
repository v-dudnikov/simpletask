import click
import time

from simpletask import Scheduler


@click.group()
@click.option("-q", "--queue", default="default", help="Queue name")
@click.pass_context
def cli(ctx, queue):
    ctx.ensure_object(dict)
    ctx.obj['queue'] = queue
    ctx.obj['sched'] = Scheduler(queue)

@cli.command()
@click.option("-i", "--interval", default=60, help="How often to check tasks")
@click.pass_context
def run(ctx, interval):
    ctx.obj['sched'].run(interval)

@cli.command()
@click.argument("command")
@click.option("-p", "--period", required=True, help="Run period seconds")
@click.option("--oneshot", is_flag=True, default=False)
@click.pass_context
def add(ctx, command, period, oneshot):
    ctx.obj['sched'].add_task(command, period, oneshot)

@cli.command()
@click.argument("id")
@click.pass_context
def delete(ctx, id):
    ctx.obj['sched'].connector.delete(int(id))

@cli.command()
@click.argument("id")
@click.option("-c", "--column", required=True)
@click.option("-v", "--value", required=True)
@click.pass_context
def update(ctx, id, column, value):
    ctx.obj['sched'].connector.update_column(id, column, value)

@cli.command()
@click.pass_context
def ps(ctx):
    row = '{id:4} {command:60} {period:8} {left:8} {oneshot:5} {enabled:5}'
    click.echo(row.format(id='id', command='command', period='period', left='left', oneshot='oneshot', enabled='enabled'))

    tasks = ctx.obj['sched'].connector.all()
    pending = 0
    for task in tasks:
        if task['enabled']:
            pending += 1

        task['left'] = '%us' % (task['period'] - (int(time.time()) - task['timestamp']))

        task['id'] = str(task['id'])
        task['period'] = '%us' % task['period']
        task['oneshot'] = str(bool(task['oneshot']))
        task['enabled'] = str(bool(task['enabled']))

        if len(task['command']) > 60:
            task['command'] = task['command'][:57] + '...'

        click.echo(row.format(**task))

    click.echo('Total %u tasks, %u are pending.' % (len(tasks), pending))
