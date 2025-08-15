from pathlib import Path
import json
from .log import logging


class FolderMeta(dict):
    def __init__(self, path: Path):

        self.path = path if path.is_file() else FolderMeta._get_metadata_path(path)

    @classmethod
    def _get_metadata_path(cls, dir: Path, suffix: str = ".meta.json"):
        metadata_filename = f".{dir.name}{suffix}"
        return dir / metadata_filename

    @classmethod
    def writeto(cls, obj, folderpath: Path):

        meta_path = cls._create_metadata_file(folderpathormetafile=folderpath)
        with open(file=meta_path, mode="w", encoding="utf-8") as f:
            json.dump(obj=obj, fp=f, indent=4, ensure_ascii=False)

    def dump(self):
        FolderMeta.writeto(obj=self, folderpath=self.path)

    @classmethod
    def loadjson(cls, folderpath: Path, suffix: str = ".meta.json"):
        metadata_path = cls._get_metadata_path(dir=folderpath, suffix=suffix)
        if metadata_path.exists():
            with open(file=metadata_path, mode="r", encoding="utf-8") as f:
                return json.load(fp=f)
        else:
            logging.error(msg=f"Metadata file {metadata_path} not found.")
            return None

    def load(self):

        obj = FolderMeta.loadjson(self.path)
        self.update(obj)  # type: ignore
        return self

    @classmethod
    def _create_metadata_file(
        cls, folderpathormetafile: Path, suffix: str = ".meta.json"
    ):
        """
        Create the metadata file if it doesn't exist.

        :return: None
        """

        # Ensure the folder exists

        # Create the metadata file if it doesn't exist

        # metadatafile =  cls.__find_metadata_file(folderpath=folderpath)
        if folderpathormetafile.is_file():
            return folderpathormetafile
        else:
            folderpathormetafile.mkdir(parents=True, exist_ok=True)
            # Create the hidden metadata file with the same name as the folder
            metadatafile = cls._get_metadata_path(
                dir=folderpathormetafile, suffix=suffix
            )
            metadatafile.touch(exist_ok=True)
            return metadatafile
