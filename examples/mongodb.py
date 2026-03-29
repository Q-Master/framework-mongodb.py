# -*- coding: utf-8 -*-
from typing import cast
from random import randint
from packets import Packet, makeField, Field
from packets.typedef.string_t import string_t
from packets.typedef.int_t import int_t
from pymongo.errors import DuplicateKeyError
from asyncframework.db.mongo import MongoCollection, makeCollection, MongoDb
from asyncframework.app import main
from asyncframework.app.script import Script
from asyncframework.util.datetime import time
from asyncframework.log import get_logger, init_logging


class TopUser(Packet):
    user_id: str = makeField(string_t, '_id', required=True)
    place: int = makeField(int_t, required=True)


class TopDB(MongoDb):
    top_users: MongoCollection[TopUser] = makeCollection(TopUser, 'TopUsers')


class Example(Script):
    log = get_logger('Example')
    def __init__(self, config):
        init_logging(True, log_level='INFO')
        super().__init__('')


    async def __start__(self):
        self.log.info('Starting example DB script')
        self.example_db = TopDB('mongodb://192.168.10.10/Example')
        await self.example_db.start(self.ioloop)

    async def __stop__(self):
        self.log.info('Stopping example DB script')
        await self.example_db.stop()

    async def __body__(self):
        start_time = time.time()
        for userid in ('User1', 'User2', 'User3'):
            user = TopUser(user_id = userid, place = randint(1, 10))
            try:
                await self.example_db.top_users.save(user)
            except DuplicateKeyError:
                self.log.error(f'Trying to insert duplicate document with ID {user.user_id}')
        documents = await self.example_db.top_users.count()
        self.log.info(f'Documents found - {documents}')
        all_records = await self.example_db.top_users.load()
        for record in all_records:
            self.log.info(f'{record.user_id} - {record.place}')
            await self.example_db.top_users.delete_one({cast(Field, TopUser.user_id).name: record.user_id})
        self.log.info(f'Time: {time.time()-start_time}')


if __name__ == '__main__':
    main(Example, None)
