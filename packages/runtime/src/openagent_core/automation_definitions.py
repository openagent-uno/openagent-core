"""Public validation and schedule projection for automation host adapters."""

def validate_graph(graph, **kwargs):
    from .workflow.validate import validate_graph as validate
    return validate(graph, **kwargs)


def validate_schedule_expression(expression, timezone=None):
    from .memory.schedule import validate_schedule_expression as validate
    return validate(expression, timezone)


def next_run_for_expression(expression, now, timezone=None):
    from .memory.schedule import next_run_for_expression as next_run
    return next_run(expression, now, timezone)


def decorate_scheduled_task(row):
    from .memory.schedule import decorate_scheduled_task as decorate
    return decorate(row)


def epoch_to_iso(value):
    from .memory.schedule import epoch_to_iso as convert
    return convert(value)


async def sync_workflow_schedules(db, workflow_id, graph, **kwargs):
    from .workflow.schedule_sync import sync_workflow_schedules as sync
    return await sync(db, workflow_id, graph, **kwargs)


def trigger_types_from_graph(graph):
    from .workflow.schedule_sync import trigger_types_from_graph as types
    return types(graph)


def block_specs():
    from .workflow.blocks import iter_block_specs
    return iter_block_specs()


def encode_execution_policy(policy):
    from .core.execution_policy import encode_execution_policy as encode
    return encode(policy)
