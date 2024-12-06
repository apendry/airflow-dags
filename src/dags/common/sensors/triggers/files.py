import asyncio
import os
import warnings
from datetime import datetime
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent


class FileIntervalTrigger(BaseTrigger):

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
            "src.common.sensors.triggers.FileIntervalTrigger",
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
                __check_interval_paths__(path, self.log, results)
            if all(results):
                yield TriggerEvent(True)
                return
            else:
                self.log.info("No File in %s", self.paths)
                await asyncio.sleep(self.poke_interval)

def __check_interval_paths__(path, log, results):
    log.info("Poking for file %s", path)
    if os.path.isfile(path):
        mod_time = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%dT%H:%M:%S")
        log.info("Found File %s last modified: %s", path, mod_time)
        results.append(True)
    else:
        log.info("No File at %s", path)
        results.append(False)
    for _, _, files in os.walk(path):
        if files:
            results.append(True)
        else:
            log.info("No File at %s", path)
            results.append(False)

class FileLatestTrigger(FileIntervalTrigger):

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize FilesTrigger arguments and classpath."""
        return (
            "src.common.sensors.triggers.FileLatestTrigger",
            {
                "paths": self.paths,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the relevant files are found."""
        while True:
            self.paths.sort(reverse=True)
            for path in self.paths:
                if os.path.isfile(path):
                    mod_time = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%dT%H:%M:%S")
                    self.log.info("Found File %s last modified: %s", path, mod_time)
                    yield TriggerEvent(path)
                    return

                for _, _, files in os.walk(path):
                    if files:
                        yield TriggerEvent(path)
                        return
            self.log.info("No Files found in %s", self.paths)
            await asyncio.sleep(self.poke_interval)
