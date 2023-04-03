# -*- coding:utf-8 -*-
import asyncio
from typing import Union, Iterable, Tuple, Dict, List, Type, Hashable
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
from asyncframework.app import Service
from asyncframework.log import get_logger
from .connection import MongoConnection
from .collection_field import MongoCollectionField
from .collection import MongoCollection


__all__ = ['MongoDb']


config_type = Union[Iterable[Union[str, dict]], Union[str, dict]]
key_type = Union[bytes, int]


class ShardObject():
    pass


class MongoDbMeta(type):
    def __new__(cls, name, bases, namespace):
        collections = {}
        for col_name, value in list(namespace.items()):
            if isinstance(value, MongoCollectionField):
                collections[col_name] = value
                del namespace[col_name]
        namespace['__collections__'] = collections
        return super().__new__(cls, name, bases, namespace)


class MongoDb(Service, metaclass=MongoDbMeta):
    """Mongo database service
    """
    __collections__: Dict[str, MongoCollectionField] = {}
    log = get_logger('typeddb')
    __pools: List[MongoConnection] = []
    __items: List[ShardObject] = []
    __sharded: bool = False

    @classmethod
    def with_collections(cls, *collections: str) -> Type['MongoDb']:
        """Constructor with filtering default list of collections
        
        Args:
            collections (tuple[str, ...]): a list of collections to leave in driver
        
        Returns:
            MongoDb: class with filtered out unused collections
        """
        collections_set = set(collections)
        namespace = {collection_name: cls.__collections__[collection_name].clone() for collection_name in collections_set}
        namespace['log'] = cls.log
        partial_class = type(cls.__name__, cls.__bases__, namespace)
        return partial_class

    def __init__(self, config: config_type):
        """Constructor

        Args:
            config (config_type): the connection config value (when sharded, iterable of values)

        Raises:
            TypeError: in case of error in config
        """
        super().__init__()
        self.__pools = []
        self.__items = []
        self.__sharded = False
        if isinstance(config, (list, tuple)):
            for element_config in config:
                if isinstance(element_config, str):
                    pool = MongoConnection(element_config)
                elif isinstance(element_config, dict):
                    pool = MongoConnection.from_host_port_db(**element_config)
                else:
                    raise TypeError(f'Config should be either str or dict, got {type(element_config)}')
                self.__pools.append(pool)
                self.__sharded = True
        elif isinstance(config, str):
            pool = MongoConnection(config)
            self.__pools.append(pool)
        elif isinstance(config, dict):
            pool = MongoConnection.from_host_port_db(**config)
            self.__pools.append(pool)
        else:
            raise TypeError(f'Config should be either str or dict, got {type(config)}')

    @property
    def sharded(self) -> bool:
        """Sharded or not

        Returns:
            bool: is sharded
        """
        return self.__sharded

    @property
    def shards(self) -> int:
        """Amount of shards. 0 if not sharded

        Returns:
            int: amount of shards
        """
        if not self.sharded:
            return 0
        return len(self.__pools)

    async def __start__(self, *args, **kwargs):
        await asyncio.gather(*[connection.start(self.ioloop) for connection in self.__pools])

        for connection in self.__pools:
            results = await asyncio.gather(*[
                self._create_ensure(connection.get_database(), coll_name, coll_info)
                for coll_name, coll_info in self.__collections__.items()
            ])
            if self.__sharded:
                shard = ShardObject()
                for collection_name, collection, collection_info in results:
                    setattr(shard, collection_name, MongoCollection(collection, collection_info))
                self.__items.append(shard)
            else:
                for collection_name, collection, collection_info in results:
                    setattr(self, collection_name, MongoCollection(collection, collection_info))

    async def __body__(self, *args, **kwargs):
        pass

    async def __stop__(self):
        await asyncio.gather(*[connection.stop() for connection in self.__pools])

    async def _create_ensure(self, database: AsyncIOMotorDatabase, coll_name: str, coll_info: MongoCollectionField) -> Tuple[str, AsyncIOMotorCollection, MongoCollectionField]:
        coll = database.get_collection(coll_info.name or coll_name)
        if coll_info.indexes:
            await coll.create_indexes(coll_info.indexes)
        return coll_name, coll, coll_info

    def __getitem__(self, key: key_type) -> ShardObject:
        if not self.__sharded:
            raise AttributeError('Not sharded DB')
        shard_id = -1
        if isinstance(key, int):
            shard_id = key
        elif isinstance(key, Hashable):
            shard_id = hash(key) % len(self.__items)
        assert 0 <= shard_id < len(self.__items)
        return self.__items[shard_id]
