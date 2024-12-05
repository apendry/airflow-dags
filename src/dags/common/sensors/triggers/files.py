import asyncio
import os
import warnings
from datetime import datetime
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent


class FilesTrigger(BaseTrigger):

    def __init__(
        self,
        paths: list,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__()
        self.paths = paths
        if kwargs.get("poll_interval") is not None:
            warnings.warn(
                "`poll_interval` has been deprecated and will be removed in future."
                "Please use `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.poke_interval: float = kwargs["poll_interval"]
        else:
            self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FilesTrigger arguments and classpath."""
        return (
            "src.common.sensors.triggers.FilesTrigger",
            {
                "paths": self.paths,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant files are found."""
        while True:
            results = []
            for path in self.paths:
                if os.path.isfile(path):
                    mod_time_f = os.path.getmtime(path)
                    mod_time = datetime.fromtimestamp(mod_time_f).strftime("%Y%m%d%H%M%S")
                    self.log.info("Found File %s last modified: %s", path, mod_time)
                    results.append(True)
                else:
                    results.append(False)
                for _, _, files in os.walk(path):
                    if files:
                        results.append(True)
                    else:
                        results.append(False)
            if all(results):
                yield TriggerEvent(True)
                return
            else:
                await asyncio.sleep(self.poke_interval)