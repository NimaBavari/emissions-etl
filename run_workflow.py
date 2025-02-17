import asyncio

from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from constants import MAIN_TASK_QUEUE, TEMPORALIO_URL
from data_converter import dataframe_converter
from workflow import EmissionsWorkflow


async def main() -> None:
    client = await Client.connect(TEMPORALIO_URL, data_converter=dataframe_converter)
    try:
        await client.execute_workflow(EmissionsWorkflow.run, id="emissions-workflow", task_queue=MAIN_TASK_QUEUE)
    except WorkflowAlreadyStartedError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
