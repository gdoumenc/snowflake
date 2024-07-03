import typing as t

from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from coworks.biz.operators import AsyncTechServicePullOperator
from coworks.biz.operators import TechMicroServiceOperator
from coworks.biz.sensors import AsyncTechMicroServiceSensor


class CoworksTaskGroup(TaskGroup):
    """ Asynchronous tasks group.

    .. versionchanged:: 0.8.4
        Added the ``start_id``, ``end_id`` properties.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transformer_task: BaseOperator | None = None
        self.call_task: BaseOperator | None = None
        self.wait_task: BaseOperator | None = None
        self.read_task: BaseOperator | None = None

    @property
    def start_id(self):
        return f'{self._group_id}.transformer' if self.transformer_task else f'{self._group_id}.call'

    @property
    def end_id(self):
        return f'{self._group_id}.read' if self.read_task else f'{self._group_id}.wait'

    @property
    def output(self):
        return self.read_task.output


def TechMicroServiceAsyncGroup(group_id: str, transformer: t.Callable = None, read: bool = True,
                               op_args: t.Collection[t.Any] | None = None,
                               op_kwargs: t.Mapping[str, t.Any] | None = None,
                               method: str = 'get', timeout: int = 900,
                               raise_errors: bool = True, raise_400_errors: bool = True,
                               xcom_push: bool = True, trigger_rule=TriggerRule.ALL_SUCCESS,
                               **tech_kwargs):
    """Task group to allow asynchronous call of a TechMicroService.

    The returned value is defined in the task_id : '{group_id}.read'.

    :param group_id: group id.
    :param transformer: python transformer function.
    :param read: do read task.
    :param op_args: python transformer args.
    :param op_kwargs: python transformer kwargs.
    :param method: microservice method called.
    :param raise_errors: raise error on client errors (default True).
    :param raise_400_errors: raise error on client 400 errors (default True).
    :param timeout: asynchronous call timeout.
    :param trigger_rule: trigger rule to be set on first task.
    :param xcom_push:  pushes result in XCom (default True).

    .. versionchanged:: 0.8.4
        Added the ``read`` parameter.
    .. versionchanged:: 0.8.4
        Added the ``trigger_rule`` parameter.
    .. versionchanged:: 0.8.0
        Added the ``transformer`` parameter.
    """
    with CoworksTaskGroup(group_id=group_id) as tg:
        if transformer:
            tg.transformer_task = PythonOperator(
                task_id='transformer',
                python_callable=transformer,
                op_args=op_args,
                op_kwargs=op_kwargs,
                trigger_rule=trigger_rule
            )

            if method.lower() == 'get':
                if 'query_params' in tech_kwargs:
                    msg = "Calling transformer method with already query_params parameter call"
                    tg.transformer_task.log.warning(msg)
                tech_kwargs['query_params'] = tg.transformer_task.output
            else:
                if 'json' in tech_kwargs:
                    tg.transformer_task.log.warning("Calling transformer method with already json parameter call")
                tech_kwargs['json'] = tg.transformer_task.output

        tg.call_task = TechMicroServiceOperator(
            task_id="call",
            asynchronous=True,
            method=method,
            raise_errors=raise_errors,
            raise_400_errors=raise_400_errors,
            trigger_rule=trigger_rule if not transformer else TriggerRule.ALL_SUCCESS,
            **tech_kwargs,
        )

        tg.wait_task = AsyncTechMicroServiceSensor(
            task_id='wait',
            cws_task_id=f'{group_id}.call',
            timeout=timeout,
        )

        if read:
            tg.read_task = AsyncTechServicePullOperator(
                task_id='read',
                cws_task_id=f'{group_id}.call',
                raise_errors=raise_errors,
                raise_400_errors=raise_400_errors,
                xcom_push=xcom_push,
            )

    tg.call_task >> tg.wait_task

    if transformer:
        tg.transformer_task >> tg.call_task
    if read:
        tg.wait_task >> tg.read_task

    return tg
