# -*- coding:utf-8 -*-
from typing import Union, Sequence, Dict, Type, Any
import asyncio
import types
from abc import ABCMeta
from motor.motor_asyncio import AsyncIOMotorDatabase
from asyncframework.app import Service
from asyncframework.log import get_logger
from .connection import MongoConnection
from .collection import MongoCollection


__all__ = ['MongoDb']


config_type = Union[Sequence[Union[str, dict]], Union[str, dict]]


class ShardObject():
    pass


class MongoDbMeta(ABCMeta):
    def __new__(cls, name, bases, namespace):
        collections = {}
        for col_name, value in list(namespace.items()):
            if isinstance(value, MongoCollection):
                collections[col_name] = value
                value.update_name(col_name)
        namespace['__collections__'] = collections
        return super().__new__(cls, name, bases, namespace)


class MongoDb(Service, metaclass=MongoDbMeta):
    """Mongo database service
    """
    __collections__: Dict[str, MongoCollection] = {}
    log = get_logger('typeddb')
    __connection: MongoConnection

    @classmethod
    def with_collections(cls, *collections: str) -> Type['MongoDb']:
        """Constructor with filtering default list of collections
        
        Args:
            collections (tuple[str, ...]): a list of collections to leave in driver
        
        Returns:
            MongoDb: class with filtered out unused collections
        """
        collections_set = set(collections)
        namespace: Dict[str, Any] = {collection_name: cls.__collections__[collection_name].clone() for collection_name in collections_set}
        namespace['log'] = cls.log
        partial_class = types.new_class(cls.__name__, cls.__bases__, exec_body=lambda ns: ns.update(namespace))
        return partial_class

    def __init__(self, config: config_type):
        """Constructor

        Args:
            config (config_type): the connection config value (when sharded, iterable of values)

        Raises:
            TypeError: in case of error in config
        """
        super().__init__()
        if isinstance(config, str):
            self.__connection = MongoConnection(config)
        elif isinstance(config, dict):
            self.__connection = MongoConnection.from_host_port(**config)
        else:
            raise TypeError(f'Config should be either str or dict, got {type(config)}')

    async def __start__(self, *args, **kwargs):
        await self.__connection.start(self.ioloop)
        db = self.__connection.get_database()
        await asyncio.gather(*[
            self._create_ensure(db, collection) for collection in self.__collections__.values()
        ])

    async def __body__(self, *args, **kwargs):
        pass

    async def __stop__(self):
        await self.__connection.stop()

    async def _create_ensure(self, database: AsyncIOMotorDatabase, collection: MongoCollection):
        coll = database.get_collection(collection.collection_name)
        collection.set_collection(coll)
        await collection.create_indexes()
