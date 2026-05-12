from .... import LOGGER
from ...ext_utils.status_utils import (
    EngineStatus,
    MirrorStatus,
    get_readable_file_size,
    get_readable_time,
)


class MergeStatus:
    def __init__(self, listener, obj, gid):
        self.listener = listener
        self._obj = obj
        self._gid = gid
        self.engine = EngineStatus().STATUS_FFMPEG

    def gid(self):
        return self._gid

    def name(self):
        return self.listener.name

    def size(self):
        return get_readable_file_size(self.listener.size)

    def processed_bytes(self):
        return get_readable_file_size(self._obj.processed_bytes)

    def progress(self):
        return f"{round(self._obj.progress_raw, 2)}%"

    def speed(self):
        return f"{get_readable_file_size(self._obj.speed_raw)}/s"

    def eta(self):
        return get_readable_time(self._obj.eta_raw) if self._obj.eta_raw else "-"

    def status(self):
        return MirrorStatus.STATUS_MERGING

    def task(self):
        return self

    async def cancel_task(self):
        LOGGER.info(f"Cancelling Merge: {self.listener.name}")
        self.listener.is_cancelled = True
        if (
            self.listener.subproc is not None
            and self.listener.subproc.returncode is None
        ):
            try:
                self.listener.subproc.kill()
            except Exception:
                pass
        await self.listener.on_upload_error("Merge stopped by user!")
