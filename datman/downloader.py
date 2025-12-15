import requests
import hashlib
from tqdm import tqdm
from pathlib import Path
import shutil
import zipfile

class Downloader:
    def __init__(self,
                 folder,
                 data_url,
                 filename,
                 extract_path,
                 data_path=None,
                 checksum=None,
                 skip_verify=False):

        self.folder = Path(folder)
        self.data_url = data_url
        self.file_path = self.folder / filename
        self.checksum = checksum
        self.extract_path = Path(extract_path)
        self.data_path = data_path

        self.skip_verify = skip_verify
        if checksum is None:
            self.skip_verify = True

    def download_and_extract(self):
        self.folder.mkdir(parents=True, exist_ok=True)

        # Download zip if not present or checksum fails
        verified = self.verify(self.file_path) if self.file_path.exists() else False
        if not self.file_path.exists() or not verified:
            if self.file_path.exists():
                print("Existing file checksum does not match, re-downloading...")

            download(
                url=self.data_url,
                file_path=self.file_path,
                verify_checksum_func=self.verify
            )
        
        self.extract()
    
    def verify(self, file_path):
        return verify_checksum(file_path, self.checksum, self.skip_verify)
    
    def extract(self):

        if not self.file_path.exists():
            # we check this before deleting anything
            raise RuntimeError(f"Cannot extract, file {self.file_path} does not exist.")

        if self.data_path and self.data_path.exists():
            # if we decided to extract, it means we want a fresh copy.
            # to avoid issues, we delete any existing data folder
            print(f"Removing existing data folder {self.data_path}...")
            shutil.rmtree(self.data_path)

        self.extract_path.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(self.file_path, "r") as zip_ref:
            file_list = zip_ref.namelist()
            for file in tqdm(file_list, desc="Extracting", disable=False):
                zip_ref.extract(file, self.extract_path)
    
################# Helper functions #################

def verify_checksum(file_path, expected_digest, skip):
    if skip:
        return True

    # If no expected digest provided, skip verification (accept existing file)
    if not expected_digest:
        return True

    # expected_digest may be like "sha256:abcd..." or just the digest (default to sha256)
    if ":" in expected_digest:
        algo, digest = expected_digest.split(":", 1)
        algo = algo.lower()
    else:
        algo = "sha256"
        digest = expected_digest

    # compute actual digest using requested algorithm
    actual = checksum(file_path, algorithm=algo)
    # normalize and compare
    return actual.lower() == digest.lower()

def checksum(file_path, algorithm="sha256"):
    # support common algorithms
    alg = algorithm.lower()
    if alg not in hashlib.algorithms_available and alg not in {"md5","sha1","sha256","sha512"}:
        # fallback to hashlib attribute if available, else raise
        try:
            hash_func = getattr(hashlib, alg)
        except AttributeError:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    else:
        # prefer direct constructor if present
        try:
            hash_func = getattr(hashlib, alg)
        except AttributeError:
            # fallback to new algorithm via hashlib.new
            def hash_func():
                return hashlib.new(alg)

    # create hasher
    hasher = hash_func() if callable(hash_func) else hashlib.new(alg)
    with open(file_path, "rb") as f:
        for chunk in tqdm(iter(lambda: f.read(8192), b""),
                          total=round(file_path.stat().st_size / 8192),
                          desc="Verifying checksum"):
            hasher.update(chunk)
    return hasher.hexdigest()

def download(url, file_path, verify_checksum_func):
    tmp_path = file_path.with_suffix(".zip.part")  # Temporary download file

    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    chunk_size = 1024

    with open(tmp_path, "wb") as f, tqdm(
        total=total_size, unit='B', unit_scale=True, desc="Downloading"
    ) as pbar:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                pbar.update(len(chunk))

    # Verify checksum before renaming
    if verify_checksum_func and not verify_checksum_func(tmp_path):
        tmp_path.unlink()  # remove incomplete/invalid download
        raise RuntimeError("Downloaded file checksum does not match! Try downloading again. If the problem persists, set skip_verify=True at your own risk.")

    tmp_path.rename(file_path)  # rename only after successful download
