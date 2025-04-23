import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import export_approved_data, load_data_to_db, normalise_and_combine_data, read_source_files
from constants import MAIN_TASK_QUEUE, TEMPORALIO_URL
from data_converter import dataframe_converter
from workflow import EmissionsWorkflow


async def main() -> None:
    client = await Client.connect(TEMPORALIO_URL, data_converter=dataframe_converter)
    worker = Worker(
        client,
        task_queue=MAIN_TASK_QUEUE,
        workflows=[EmissionsWorkflow],
        activities=[
            read_source_files,
            normalise_and_combine_data,
            load_data_to_db,
            export_approved_data,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
