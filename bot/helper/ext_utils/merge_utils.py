from asyncio import create_subprocess_exec, ensure_future, gather, sleep
from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, remove
from natsort import natsorted
from os import path as ospath, walk
from time import time

from ... import LOGGER, task_dict, task_dict_lock
from ...core.config_manager import BinConfig
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.files_utils import get_path_size
from ..ext_utils.media_utils import get_document_type
from ..mirror_leech_utils.status_utils.merge_status import MergeStatus
from ..telegram_helper.message_utils import update_status_message


class MergeVideos:
    def __init__(self, listener):
        self._listener = listener
        self._processed_bytes = 0
        self._start_time = time()

    @property
    def processed_bytes(self):
        return self._processed_bytes

    @property
    def speed_raw(self):
        elapsed = time() - self._start_time
        return self._processed_bytes / elapsed if elapsed > 0 else 0

    @property
    def progress_raw(self):
        try:
            return self._processed_bytes / self._listener.size * 100
        except ZeroDivisionError:
            return 0

    @property
    def eta_raw(self):
        try:
            remaining = self._listener.size - self._processed_bytes
            return remaining / self.speed_raw
        except (ZeroDivisionError, ValueError):
            return 0

    async def _track_progress(self, outfile):
        while not self._listener.is_cancelled:
            await sleep(1)
            if await aiopath.exists(outfile):
                self._processed_bytes = await get_path_size(outfile)

    async def merge(self, path, gid):
        video_files, remove_files = [], []
        total_size = 0

        for dirpath, _, files in await sync_to_async(walk, path):
            for file in natsorted(files):
                fpath = ospath.join(dirpath, file)
                is_video, _, _ = await get_document_type(fpath)
                if is_video:
                    fsize = await get_path_size(fpath)
                    total_size += fsize
                    video_files.append(f"file '{fpath}'")
                    remove_files.append(fpath)

        if len(video_files) <= 1:
            return True

        name = ospath.basename(path)
        self._listener.size = total_size

        async with task_dict_lock:
            task_dict[self._listener.mid] = MergeStatus(self._listener, self, gid)
        await update_status_message(self._listener.message.chat.id)

        input_file = ospath.join(path, "madara.txt")
        async with aiopen(input_file, "w") as f:
            await f.write("\n".join(video_files))

        outfile = f"{ospath.join(path, name)}.mkv"

        cmd = [
            BinConfig.FFMPEG_NAME,
            "-ignore_unknown",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            input_file,
            "-map",
            "0",
            "-c",
            "copy",
            outfile,
        ]

        self._listener.subproc = await create_subprocess_exec(*cmd)
        tracker = ensure_future(self._track_progress(outfile))
        code = await self._listener.subproc.wait()
        tracker.cancel()

        if self._listener.is_cancelled or code == -9:
            return False

        if code == 0:
            await remove(input_file)
            if not self._listener.seed:
                await gather(*[remove(f) for f in remove_files])
            LOGGER.info(f"Merged successfully: {name}.mkv")
        else:
            LOGGER.error(f"Merge failed for: {name}.mkv")

        return True
