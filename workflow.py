from datetime import timedelta

from temporalio import workflow

from constants import EXPORT_CSV_PATH

with workflow.unsafe.imports_passed_through():
    from activities import (
        export_approved_data,
        load_data_to_db,
        normalise_and_combine_data,
        read_source_files,
    )


@workflow.defn
class EmissionsWorkflow:
    @workflow.run
    async def run(self) -> None:
        results = await workflow.execute_activity(read_source_files, start_to_close_timeout=timedelta(seconds=15))
        combined_data = await workflow.execute_activity(
            normalise_and_combine_data, args=[results], start_to_close_timeout=timedelta(seconds=15)
        )
        await workflow.execute_activity(
            load_data_to_db, args=[combined_data], start_to_close_timeout=timedelta(seconds=15)
        )
        await workflow.execute_activity(
            export_approved_data, args=[EXPORT_CSV_PATH], start_to_close_timeout=timedelta(seconds=15)
        )
