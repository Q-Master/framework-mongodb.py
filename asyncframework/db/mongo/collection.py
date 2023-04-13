# -*- coding:utf-8 -*-
from typing import Union, Optional, TypeVar, Generic, List, Tuple, Sequence, Dict, Any
import asyncio
from pymongo import ReturnDocument, ASCENDING, DESCENDING
from pymongo.cursor import CursorType
from pymongo.collation import Collation
from pymongo.client_session import ClientSession
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorCursor
from asyncframework.log import get_logger
from packets import PacketBase
from packets import TablePacket
from .collection_field import MongoCollectionField


__all__ = ['MongoCollection']


T = TypeVar('T', bound=PacketBase)


class MongoCollection(Generic[T]):
    """MongoDb collection class
    """
    log = get_logger('typed_collection')
    _collection: AsyncIOMotorCollection
    _collection_info: MongoCollectionField
    _cursor: Optional[AsyncIOMotorCursor] = None
    _projection: List[str]

    @property
    def default_filter(self) -> Optional[Dict[str, Any]]:
        """The default filter for all the documents of a collection

        Returns:
            Optional[Dict[str, Any]]: default filter or None if not set
        """
        return self._collection_info.default_filter

    @property
    def collection_name(self) -> str:
        """Current collection name

        Returns:
            str: the name of collection
        """
        return self._collection.name

    def __init__(self, collection: AsyncIOMotorCollection, collection_info: MongoCollectionField) -> None:
        """Constructor

        Args:
            collection (AsyncIOMotorCollection): the collection driver class
            collection_info (CollectionField): the collection higher-level description
        """
        assert issubclass(collection_info.record_type, PacketBase)
        self._collection = collection
        self._collection_info = collection_info
        self._cursor = None
        self._projection = [] if issubclass(collection_info.record_type, TablePacket) else [field for field in self._collection_info.record_type.__raw_mapping__.keys()]

    def __getattr__(self, item):
        return getattr(self._collection, item)

    def cursor(self, filter: dict, tailable: bool = False, await_data: bool = False, **find_args) -> 'MongoCollection[T]':
        """Wrapper over find with cursor.

        Args:
            filter (dict): same as in find, load, etc...
            tailable (bool, optional): is the collection tailable. Defaults to False.
            await_data (bool, optional): if we need to wait for data. Defaults to False.

        Returns:
            Collection[T]: the clone of this collection with cursor set.
        """
        if 'cursor' in find_args.keys():
            self.log.warn('\'cursor\' is not permitted in additional args')
            find_args.pop('cursor')
        if 'fields' in find_args.keys():
            self.log.warn('\'fields\' is not permitted in additional args')
            find_args.pop('fields')
        if 'cursor' in find_args.keys():
            self.log.warn('\'cursor\' is not permitted in additional args')
            find_args.pop('cursor')
        cloned: MongoCollection[T] = MongoCollection(self._collection, self._collection_info)
        _filter = dict(self._collection_info.default_filter, **filter)
        if tailable:
            find_args['cursor_type'] = CursorType.TAILABLE
        if tailable and await_data:
            find_args['cursor_type'] = CursorType.TAILABLE_AWAIT
            find_args['await_data'] = True
        cloned._cursor = cloned._collection.find(filter=_filter, projection=cloned._projection, **find_args)
        return cloned

    async def next(self):
        """Fetch next record from cursor

        Raises:
            RuntimeError: if cursor is not set

        Yields:
            T: next record mapped to `CollectionField.record_type`
        """
        if not self._cursor:
            raise RuntimeError('Cursor not enabled. Use cursor() call.')
        result = None
        try:
            async for doc in self._cursor:
                result = self._collection_info.record_type.load(doc, strict=self._collection_info.strict)
                yield result
        except StopIteration:
            self._cursor = None
            raise
        yield None

    @property
    def alive(self) -> bool:
        """If cursor is alive

        Returns:
            bool: aliveness of a cursor
        """
        if self._cursor:
            return self._cursor.alive
        return False

    async def load(self, filter: dict, **find_args) -> List[T]:
        """Load some data from collection

        Args:
            filter (dict): the additional filter properties.

        Raises:
            RuntimeError: if cursor is set

        Returns:
            List[T]: the result of query, mapped to `CollectionField.record_type`
        """
        if 'fields' in find_args.keys():
            self.log.warn('\'fields\' is not permitted in additional args')
            find_args.pop('fields')
        if 'filter' in find_args.keys():
            self.log.warn('\'filter\' is not permitted in additional args')
            find_args.pop('filter')
        if 'cursor' in find_args.keys():
            self.log.warn('\'cursor\' is not permitted in additional args. Use cursor()/next() instead')
            raise RuntimeError('Use cursor()/next() instead')
        _filter = dict(self._collection_info.default_filter, **filter)
        result = []
        async for data in self._collection.find(filter=_filter, projection=self._projection, **find_args):
            result.append(self._collection_info.record_type.load(data, strict=self._collection_info.strict))
        return result

    async def load_one(self, filter: dict, **find_args) -> Optional[T]:
        """Load one record from collection

        Args:
            filter (dict): the additional filter properties.

        Raises:
            RuntimeError: if cursor is set

        Returns:
            Optional[T]: the result of query, mapped to `CollectionField.record_type`
        """
        if 'fields' in find_args.keys():
            self.log.warn('\'fields\' is not permitted in additional args')
            find_args.pop('fields')
        if 'filter' in find_args.keys():
            self.log.warn('\'filter\' is not permitted in additional args')
            find_args.pop('filter')
        if 'limit' in find_args.keys():
            self.log.warn('\'limit\' is not permitted in additional args')
            find_args.pop('limit')
        if 'cursor' in find_args.keys():
            self.log.warn('\'cursor\' is not permitted in additional args. Use cursor()/next() instead')
            raise RuntimeError('Use cursor()/next() instead')
        result = None
        if self._cursor:
            try:
                if (await self._cursor.fetch_next):
                    result = self._cursor.next_object()
                else:
                    self._cursor = None
            except StopIteration:
                self._cursor = None
                raise
        else:
            _filter = dict(self._collection_info.default_filter, **filter)
            result = await self._collection.find_one(filter=_filter, projection=self._projection, **find_args)
        if result:
            return self._collection_info.record_type.load(result, strict=self._collection_info.strict)
        return None

    async def find(self, 
        filter: Optional[dict] = None, 
        projection: Optional[list] = None, 
        skip: int = 0, 
        limit: int = 0, 
        sort: Optional[List[Tuple[str, int]]]=None, 
        cursor_type: int = CursorType.NON_TAILABLE, 
        **find_args) -> List[dict]:
        """Query the collection

        Args:
            filter (Optional[dict], optional): additional filter properties. Defaults to None.
            projection (Optional[list], optional): a list of field names which should be returned from a query. Defaults to None.
            skip (int, optional): the number of documents to skip. Defaults to 0.
            limit (int, optional): the maximum amount of documents in result. Defaults to 0.
            sort (Optional[List[Tuple[str, int]]], optional): a list of name: `ASCENDING` | `DESCENDING` tuples to sort the query. Defaults to None.
            cursor_type (int, optional): the cursor type. See `CursorType`. Defaults to CursorType.NON_TAILABLE.

        Returns:
            List[dict]: the documents of query
        """
        _filter = dict(self._collection_info.default_filter, **(filter or {}))
        cursor = self._collection.find(_filter, projection, skip=skip, limit=limit, sort=sort, cursor_type=cursor_type, **find_args)
        return await cursor.to_list(None)

    async def find_one(self, filter: Optional[dict] = None, projection: Optional[list] = None, **kwargs) -> Optional[dict]:
        """Query one document from collection.

        Args:
            filter (Optional[dict], optional): additional filter properties. Defaults to None.
            projection (Optional[list], optional): a list of field names which should be returned from a query. Defaults to None.

        Returns:
            Optional[dict]: the found document or None
        """
        _filter = dict(self._collection_info.default_filter, **(filter or {}))
        return await self._collection.find_one(_filter, projection, **kwargs)

    async def count(self, filter: Optional[dict] = None, projection: Optional[list] = None) -> int:
        _filter = dict(self._collection_info.default_filter, **(filter or {}))
        return await self._collection.count_documents(_filter, projection)

    async def save(self, data: Union[Sequence[T], T]):
        """Insert the packet to a collection

        Args:
            data (Union[Sequence[T], T]): Sequence of packets or single packet to inser to collection
        """
        if isinstance(data, Sequence):
            if self._collection_info.incremental_ids:
                await asyncio.gather((self._next_id(d) for d in data if d['_id'] is None))
            data_to_store = [d.dump() for d in data]
            await self._collection.insert_many(data_to_store, ordered=False)
        else:
            if self._collection_info.incremental_ids and data['_id'] is None:
                await self._next_id(data)
            data_to_store = data.dump()
            await self._collection.insert_one(data_to_store)

    async def store(self, 
        filter: dict, 
        data: Union[Sequence[T],  T], 
        upsert=False, 
        array_filters=Optional[List[Dict[str, Any]]], 
        bypass_document_validation=False, 
        collation=Optional[Union[Dict[str, Any], Collation]], 
        session=Optional[ClientSession]) -> int:
        """Update the document(s) in the collection

        Args:
            filter (dict): additional filter properties
            data (Union[Sequence[T],  T]): Sequence of packets or a single packet to update in collection
            upsert (bool, optional): insert the new documents if not found. Defaults to False.
            array_filters (Optional[List[Dict[str, Any]]], optional): a list of filters specifying which array elements an update should apply. Requires MongoDB 3.6+.. Defaults to None.
            bypass_document_validation (bool, optional): allows the write to opt-out of document level validation. Defaults to False.
            collation (Optional[Union[Dict[str, Any], Collation]], optional): an instance of `Collation`. Defaults to None.
            session (Optional[ClientSession], optional): a `ClientSession`, created with `start_session()`. Defaults to None.
        
        Returns:
            int: amount of documents modified
        """
        if isinstance(data, Sequence):
            data_to_store = [d.dump() for d in data]
            result = await self._collection.update_many(filter, {"$set": data_to_store}, upsert=upsert, array_filters=array_filters, bypass_document_validation=bypass_document_validation, collation=collation, session=session)
        else:
            data_to_store = data.dump()
            result = await self._collection.update_one(filter, {"$set": data_to_store}, upsert=upsert, array_filters=array_filters, bypass_document_validation=bypass_document_validation, collation=collation, session=session)
        return result.modified_count

    async def _next_id(self, d: T):
        if self._collection_info.incremental_ids:
            res = await self._collection.find_one_and_update(
                {'_id': self._collection_info.name},
                {'$inc': {'seq': 1}},
                projection={'seq': True, '_id': False},
                return_document=ReturnDocument.AFTER
            )
            d['_id'] = res['seq']
        raise RuntimeError('CollectionField must be set as incremental_ids=True')
