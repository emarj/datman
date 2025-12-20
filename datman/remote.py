from typing import Union
from pathlib import Path

from .downloader import SupportedArchives


class Remote:
    url : str
    filename : str
    root_folder : Path
    checksum : Union[str,None]
    archive_type : Union[SupportedArchives,None]
    
    def __init__(self, url: str, filename: str, root_folder: Union[Path,str] = "", checksum: Union[str,None] = None, archive_type: Union[SupportedArchives,None] = None) -> None:
        self.url = url
        self.filename = filename
        self.root_folder = Path(root_folder)
        self.checksum = checksum
        self.archive_type = archive_type