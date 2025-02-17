import json

import pandas as pd
from temporalio.api.common.v1 import Payload
from temporalio.converter import DataConverter, EncodingPayloadConverter


class PandasPayloadConverter(EncodingPayloadConverter):
    def __init__(self) -> None:
        super().__init__()
        self._dataframe_marker = "__df__"

    @property
    def encoding(self) -> str:
        return "binary/pandas"

    def _recursive_to_serializable(self, obj: object) -> object:
        if isinstance(obj, pd.DataFrame):
            return {
                self._dataframe_marker: True,
                "columns": list(obj.columns),
                "data": obj.values.tolist(),
                "index": obj.index.tolist(),
            }
        if isinstance(obj, dict):
            return {k: self._recursive_to_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._recursive_to_serializable(v) for v in obj]
        return obj

    def _recursive_from_serializable(self, obj: object) -> object:
        if isinstance(obj, dict) and self._dataframe_marker in obj:
            return obj
        if isinstance(obj, dict):
            return {k: self._recursive_from_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._recursive_from_serializable(v) for v in obj]
        return obj

    def to_payload(self, value: object) -> Payload:
        serialized = self._recursive_to_serializable(value)
        return Payload(metadata={"encoding": self.encoding.encode()}, data=json.dumps(serialized).encode())

    def from_payload(self, payload: Payload) -> object:
        decoded = json.loads(payload.data.decode())
        return self._recursive_from_serializable(decoded)

    def to_payloads(self, values: list[object]) -> list[Payload]:
        return [self.to_payload(v) for v in values]

    def from_payloads(self, payloads: list[Payload], type_hints: object = None) -> list[object]:
        return [self.from_payload(p) for p in payloads]


class DataframeConverter(DataConverter):
    def __init__(self) -> None:
        super().__init__(payload_converter_class=PandasPayloadConverter)


dataframe_converter = DataframeConverter()
