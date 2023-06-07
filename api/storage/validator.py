from fastapi import UploadFile, HTTPException
import schemas
from .model import Storage
from config import settings

CONTENT_JSON = {'content-type': 'application/json'}

class Validator:

    def __init__(self, is_test: bool):
        self.storage = Storage(is_test)
        return

    async def create_file(self, file: UploadFile) -> schemas.File:

        if self.storage.file_integrity(file.filename):
            raise HTTPException(409, "File already exists", CONTENT_JSON)
        
        file.read = Validator.register_size_checking_hook(file.read)
        
        return await self.storage.create_file(file)


    async def retrieve_file(self, filename: str) -> bytes:

        if not self.storage.file_integrity(filename):
            raise HTTPException(404, "File not Found", CONTENT_JSON)

        return await self.storage.retrieve_file(filename)

    async def update_file(self, file: UploadFile) -> schemas.File:
        
        if not self.storage.file_integrity(file.filename):
            raise HTTPException(404, "File not Found", CONTENT_JSON)

        file.read = Validator.register_size_checking_hook(file.read)
        
        return await self.storage.update_file(file)


    def delete_file(self, filename: str) -> None:

        if not self.storage.file_integrity(filename):
            raise HTTPException(404, "File not Found", CONTENT_JSON)

        return self.storage.delete_file(filename)
    
    def fix_block(self, block_id: int) -> None:
        return self.storage.fix_block(block_id)

    @staticmethod
    def register_size_checking_hook(read_func):

        async def wrapper(size: int = -1):
            tem = await read_func(size)
            if len(tem) > settings.MAX_SIZE:
                raise HTTPException(413, "File too large", CONTENT_JSON)
            return tem
        
        return wrapper
