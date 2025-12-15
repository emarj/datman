import enum
from pathlib import Path

from .downloader import Downloader
from .kv_store import load_kv, save_kv

class Status(enum.Enum):
    NONE = "NONE"
    OK = "OK"


class DataManager:
    def __init__(
        self,
        root,
        version_type_str,
        remote,
        extract_subpath=None,
        from_scratch=False,
        patch_map=None,
        skip_verify=False,
    ):

        self.version_type = version_type_str

        self.root = Path(root)
        if extract_subpath is None:
            extract_subpath = ""
        extract_path = self.root / extract_subpath

        if not self.root.exists():
            self.root.mkdir(parents=True, exist_ok=False)

        write_readme(self.root)

        self.data_path = extract_path / remote["folder_name"]
        self.status_file_path = self.root / "STATUS"

        self.dv = Downloader(
            folder=self.root,
            data_url=remote["url"],
            filename=remote["filename"],
            checksum=remote["checksum"],
            extract_path=extract_path,
            data_path=self.data_path,
            skip_verify=skip_verify,
        )

        self.patch_map = patch_map if patch_map is not None else {}

        ##### Setup #####

        if from_scratch:
            self.set_status(Status.NONE)

        self._download_and_extract()

    

    def _download_and_extract(self):
        status = self.get_status()
        if status == Status.OK:
            return
        
        self.dv.download_and_extract()

        self._apply_patches()

        self.set_status(Status.OK)

        print(f"Dataset version {self.version_type} is ready")


    def _apply_patches(self) -> None:
        # patches_map = {
        #     'v2.0.1_typeA': [patch_2ds_v2_0_1],
        #     'v2.0.2_typeA': [patch_2ds_v2_0_1, patch_2ds_v2_0_2],
        #     'v3.0.1_typeB': [patch_2ds_v3_0_1],
        # }

        patches = self.patch_map.get(str(self.version_type), [])

        if len(patches) == 0:
            print("No patches to apply.")
            return
        
        print(f"Applying {len(patches)} patches...")

        for patch_func in patches:
            patch_func(str(self.data_path))

    ######## Status management ########

    def get_status(self):
        if not self.status_file_path.exists():
            return Status.NONE

        try:
            statuses = load_kv(self.status_file_path)
        except Exception as _:
            print("Corrupted status file. Deleting it.")
            self.status_file_path.unlink(missing_ok=True)
            return Status.NONE
        status_str = statuses.get(str(self.version_type), "NONE")
        try:
            return Status(status_str)
        except ValueError:
            print(f"Unknown status '{status_str}' in status file. Treating as NONE.")
            return Status.NONE

    def set_status(self, status: Status):
        statuses = {}
        if self.status_file_path.exists():
            try:
                statuses = load_kv(self.status_file_path)
            except Exception as _:
                print("Corrupted status file. Creating a new one.")
        
        statuses[str(self.version_type)] = status.value
        save_kv(self.status_file_path, statuses)

##### README to insert into root folder #####

README = """TLDR: DO NOT TOUCH the contents of this folder unless you know what you are doing.

This folder is managed by a dataset data manager, which handles downloading, verifying, extracting files.
If something do not work as expected, try to use `from_scratch=True` option or delete the `STATUS` file to force re-download and re-extraction.

This behaviour can be disabled by setting unmanaged mode in the dataset, but then you are responsible for having the correct data in place.

If you want to free space, you can delete the zip files after a *successful* extraction, they will be re-downloaded automatically if missing.
You can delete the extracted data folders for versions you do not need anymore, the major version folder (v2) is always necessary."""

def write_readme(folder_path):
    with open(folder_path / "README", "w") as f:
        f.write(README)