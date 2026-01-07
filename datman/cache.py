from abc import abstractmethod
from typing import Union
from pathlib import Path
from .kv_store import save_kv, load_kv

class IOBackend:
    @abstractmethod
    def save(self, data: dict, path: Path) -> None:
        raise NotImplementedError
    @abstractmethod
    def load(self, path: Path) -> dict:
        raise NotImplementedError
    @abstractmethod
    def extension(self) -> str:
        raise NotImplementedError
    
    def _add_extension_if_missing(self, path: Path) -> Path:
        if not path.suffix == self.extension():
            path = Path(f"{path}{self.extension()}")
        return path


class NumpyBackend(IOBackend):
    def __init__(self, allow_pickle: bool = True) -> None:
        import numpy as np # type: ignore
        self.np = np
        
        self.allow_pickle = allow_pickle

    def save(self, data: dict, path: Path) -> None:
        self.np.save(self._add_extension_if_missing(path), data, allow_pickle=self.allow_pickle) # type: ignore

    def load(self, path: Path) -> dict:
        return self.np.load(self._add_extension_if_missing(path), allow_pickle=self.allow_pickle).item()
    def extension(self) -> str:
        return ".npy"
    
class TorchBackend(IOBackend):
    def __init__(self) -> None:
        try:
            import torch # type: ignore
            self.torch = torch
        except ImportError:
            raise ImportError("PyTorch is not installed. Please install it to use the 'torch' backend.")
        

    def save(self, data: dict, path: Path) -> None:
        self.torch.save(data, self._add_extension_if_missing(path))

    def load(self, path: Path) -> dict:
        return self.torch.load(self._add_extension_if_missing(path))
    def extension(self) -> str:
        return ".pt"

class SimpleCache():
    def __init__(self,
                 root: Union[str, Path],
                 backend: IOBackend,
                 keep_in_memory: bool = True,
                 ):
        
        self.backend = backend
        self.root = Path(root)
        self.data_path = self.root / "data"
        self.index_path = self.root / "index.kv"
        self.keep_in_memory = keep_in_memory
        self.cache = {}
        self.index = {}

        self.root.mkdir(parents=True, exist_ok=True)
        self.data_path.mkdir(parents=True, exist_ok=True)

        if self.index_path.exists():
            self.index = load_kv(self.index_path,True)


    def __len__(self) -> int:
        return len(self.index)
    
    def __getitem__(self, key: Union[str, int]) -> dict:
        return self.load(key)
    
    def __setitem__(self, key: Union[str, int], data: dict) -> None:
        self.save(key, data)

    
    def _save(self, key: Union[str, int], data: dict) -> None:
        if isinstance(key, int):
            self.index[key] = f'sample_{key}'
        else:
            self.index[len(self.index)] = key if isinstance(key, str) else str(key)

        data_path = self.data_path / f"{key}"
        self.backend.save(data, data_path)
        if self.keep_in_memory:
            self.cache[key] = data
        
        
    def save(self, key: Union[str, int], data: dict) -> None:
        self._save(key, data)
        
        self.save_index()
    
    def save_index(self) -> None:
        save_kv(self.index_path, self.index)
            
    def load(self, key: Union[str, int]) -> dict:
        if isinstance(key, int):
            key = self.index[key]

        if key in self.cache:
            return self.cache[key]

        data_path = self.data_path / f"{key}"
        data = self.backend.load(data_path)

        if self.keep_in_memory:
            self.cache[key] = data
        return data