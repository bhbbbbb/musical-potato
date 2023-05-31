from typing import Iterable
import os
import hashlib
from pathlib import Path
from typing import List

import schemas
from config import settings
from fastapi import UploadFile
from loguru import logger


class Storage:
    def __init__(self, is_test: bool):
        self.is_test = is_test
        self.block_path: List[Path] = [
            self.__get_block_path(i) for i in range(settings.NUM_DISKS)
        ]
        self.__create_block()

    def __get_block_path(self, i: int):
        return (
            Path("/tmp") / f"{settings.FOLDER_PREFIX}-{i}-test"
                if self.is_test
                else Path(settings.UPLOAD_PATH) / f"{settings.FOLDER_PREFIX}-{i}"
        )
    
    def __create_block(self):
        for path in self.block_path:
            logger.warning(f"Creating folder: {path}")
            path.mkdir(parents=True, exist_ok=True)

    @property
    def num_blocks(self):
        return len(self.block_path)

    def file_integrity(self, filename: str) -> bool:
        """check if file integrity is valid
        file integrated must satisfy following conditions:
            1. all data blocks must exist
            2. size of all data blocks must be equal
            3. parity block must exist
            4. parity verify must success

        if one of the above conditions is not satisfied
        the file does not exist
        and the file is considered to be damaged
        so we need to delete the file
        """
        return self.__file_integrity(filename) or (self.delete_file(filename) or False)

    def __file_integrity(self, filename: str) -> bool:
        
        pathes = [block_path / filename for block_path in self.block_path]

        """all data blocks and parity block must exist"""
        for path in pathes:
            if not os.path.isfile(path):
                logger.info(f'path does not exist"{path}"')
                return False
        

        """size of all data blocks must be equal"""
        size = os.path.getsize(pathes[0])
        for path in pathes[1:]:
            if size != os.path.getsize(path):
                logger.info(f'find different sizes which are ({size}, {os.path.getsize(path)})')
                return False


        def reader(file_path):
            with open(file_path, 'rb') as fin:
                while b := fin.read(1):
                    yield int.from_bytes(b, 'big')

        readers = [reader(path) for path in pathes]

        for c in Storage.__get_parity_array(readers):
            if c != 0:
                logger.info('parity check failed.')
                return False
            
        return True

    async def create_file(self, file: UploadFile) -> schemas.File:

        content = await file.read()
        overall_size = len(content)
        d, m = divmod(overall_size, self.num_blocks - 1)
        
        segs: list[bytes] = []
        sizes = [(d + 1) if i < m else d for i in range(self.num_blocks - 1)]
        probe = 0
        segs = [content[probe: (probe := probe + s)] for s in sizes]
        for i in range(m):
            segs[~i] += b'\x00'
        
        
        parity = Storage.__get_parity(segs)
        segs.append(parity)

        for seg, block_path in zip(segs, self.block_path):
            with open(block_path / file.filename, 'wb') as fout:
                fout.write(seg)

        return schemas.File(
            name=file.filename,
            size=len(content),
            checksum=hashlib.md5(content).hexdigest(),
            content=content.decode(),
            content_type=file.content_type,
        )

    async def retrieve_file(self, filename: str) -> bytes:
        
        def gen():
            for block_path in self.block_path[:-1]:
                with open(block_path / filename, 'br') as fin:
                    content = fin.read()
                    yield content[:-1] if content.endswith(b'\x00') else content

        return b''.join(gen())

    async def update_file(self, file: UploadFile) -> schemas.File:

        self.delete_file(file.filename)
        return await self.create_file(file)

    def delete_file(self, filename: str) -> None:
        for block_path in self.block_path:
            path = block_path / filename
            os.path.isfile(path) and os.remove(path)
        return

    def fix_block(self, block_id: int) -> None:
        
        block_to_fix = self.__get_block_path(block_id)
        ok_blocks = [path for path in self.block_path if path != block_to_fix]
        files_to_fix = os.listdir(ok_blocks[0])
        
        def file_reader(path):
            with open(path, 'rb') as fin:
                return fin.read()

        for file in files_to_fix:
            contents = [file_reader(block_path / file) for block_path in ok_blocks]
            parity = Storage.__get_parity(contents)
            with open(block_to_fix / file, 'wb') as fout:
                fout.write(parity)

    @staticmethod
    def __get_parity(segmentations: Iterable[bytes]):
        return bytes(Storage.__get_parity_array(segmentations))

    @staticmethod
    def __get_parity_array(segmentations: Iterable[bytes]):
        def xor(bytes: tuple[int]):
            res = bytes[-1]
            for b in bytes[:-1]:
                res ^= b
            return res
        return (xor(bs) for bs in zip(*segmentations))



