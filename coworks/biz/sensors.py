from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.base import poke_mode_only
from airflow.utils.decorators import apply_defaults

from coworks.biz.operators import TechMicroServiceOperator


@apply_defaults
@poke_mode_only
class TechMicroServiceSensor(BaseSensorOperator, TechMicroServiceOperator):
    """Sensor to call a TechMicroservice until call succeed.

    Same parameters as TechMicroServiceOperator except 'asynchronous' and 'raise_400_errors'
    """

    def __init__(self, poke_interval: float = 30, **kwargs):
        super().__init__(asynchronous=False, raise_400_errors=False, poke_interval=poke_interval, **kwargs)

    def poke(self, context):
        """Set microservice context and execute it eache time."""
        self.pre_execute(context)
        res = self._call_cws(context)
        return res.ok


@apply_defaults
class AsyncTechMicroServiceSensor(BaseSensorOperator):
    """Sensor to wait until an asynchronous TechMicroservice call is ended.

    :param cws_task_id: the tech microservice task_id awaited.
    :param aws_conn_id: AWS S3 connection.
    """

    def __init__(self, *, cws_task_id: str, aws_conn_id: str = 'aws_s3', **kwargs):
        super().__init__(**kwargs)
        self.cws_task_id = cws_task_id
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        self.log.info(f"Waiting for {self.cws_task_id} result")

        # Get bucket information to poke
        ti = context['ti']
        bucket_name = ti.xcom_pull(task_ids=self.cws_task_id, key='bucket')
        self.log.info(f"bucket_name: {bucket_name}")
        bucket_key = ti.xcom_pull(task_ids=self.cws_task_id, key='key')
        self.log.info(f"bucket_key: {bucket_key}")

        if not bucket_name or not bucket_key:
            return False

        # For dynamic tasks, the xcom are stored in a list
        self.log.info(f"Map index: {ti.map_index}")
        if ti.map_index >= 0:
            try:
                bucket_name = bucket_name[ti.map_index]
                bucket_key = bucket_key[ti.map_index]
            except IndexError:
                # May occurs when all the mapped results are not completed
                return False

        self.log.info(f"Poking for key : s3://{bucket_name}/{bucket_key}")
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        return hook.check_for_key(bucket_key, bucket_name)
