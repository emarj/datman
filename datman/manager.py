from typing import Callable, List, Union
import enum
from pathlib import Path

from .downloader import Downloader
from .kv_store import load_kv, save_kv
from .remote import Remote

class Status(enum.Enum):
    NONE = "NONE"
    OK = "OK"


class DataManager:

    root : Path
    dataset_id : str
    dv : Downloader
    data_path : Path
    status_file_path : Path
    patches : list

    def __init__(
        self,
        root : Union[str,Path],
        dataset_id: str,
        remote : Remote,
        download_folder: Union[str,Path] = "",
        extract_subpath: Union[str,Path] = "",
        from_scratch: bool = False,
        patches : List[Callable[[str], None]] = [],
        skip_verify: bool = False,
    ):

        self.dataset_id = dataset_id
        self.root = Path(root)
        self.patches = patches

        extract_path = self.root / extract_subpath
        self.data_path = extract_path / remote.root_folder
        self.status_file_path = self.root / "STATUS"

        if not self.root.exists():
            self.root.mkdir(parents=True, exist_ok=False)

        self.write_readme()

        if from_scratch:
            self.set_status(Status.NONE)



        self.dv = Downloader(
            folder=Path(download_folder) if download_folder else self.root,
            data_url=remote.url,
            filename=remote.filename,
            checksum=remote.checksum,
            archive_type=remote.archive_type,
            extract_path=extract_path,
            data_path=self.data_path,
            skip_verify=skip_verify,
        )

        self._download_and_extract()

    

    def _download_and_extract(self) -> None:
        status = self.get_status()
        if status == Status.OK:
            return
        
        self.dv.download_and_extract()
        
        self._apply_patches()

        self.set_status(Status.OK)

        print(f"Dataset version {self.dataset_id} is ready")


    def _apply_patches(self) -> None:
        if self.patches is None:
            print("No patches to apply.")
            return
        
        print(f"Applying {len(self.patches)} patches...")
        for patch_func in self.patches:
            if not callable(patch_func):
                raise ValueError(f"Patch {patch_func} is not callable")
            patch_func(str(self.data_path))

    ######## Status management ########

    def get_status(self) -> Status:
        if not self.status_file_path.exists():
            return Status.NONE

        try:
            statuses = load_kv(self.status_file_path)
        except Exception as _:
            print("Corrupted status file. Deleting it.")
            self.status_file_path.unlink(missing_ok=True)
            return Status.NONE
        status_str = statuses.get(self.dataset_id, "NONE")
        try:
            return Status(status_str)
        except ValueError:
            print(f"Unknown status '{status_str}' in status file. Treating as NONE.")
            return Status.NONE

    def set_status(self, status: Status) -> None:
        statuses = {}
        if self.status_file_path.exists():
            try:
                statuses = load_kv(self.status_file_path)
            except Exception as _:
                print("Corrupted status file. Creating a new one.")
        
        statuses[self.dataset_id] = status.value
        save_kv(self.status_file_path, statuses)

    def write_readme(self) -> None:
        with open(self.root / "README", "w") as f:
            f.write(README)

##### README to insert into root folder #####

README = """TLDR: DO NOT TOUCH the contents of this folder unless you know what you are doing.

This folder is managed by a dataset data manager, which handles downloading, verifying and extracting files.
If something does not work as expected, try to use `from_scratch=True` option or delete the `STATUS` file to force re-download and re-extraction.

If you want to free up space, you can delete the zip files after a *successful* extraction, they will be re-downloaded automatically if needed.

You can delete the extracted data folders for versions you do not need anymore, but they have to be deleted from the STATUS file as well.
Be careful that some versions might be required by others as base data."""

