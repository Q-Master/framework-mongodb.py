# -*- coding:utf-8 -*-
from typing import List, Iterable, Optional, Dict, Any
from pymongo.operations import IndexModel
from packets.packet import PacketBase, Packet


__all__ = ['MongoCollectionField']


class Dummy(Packet):
    pass


class MongoCollectionField():
    """Class for representing collections in database models
    """
    def __init__(self, record_type: type[PacketBase], name: Optional[str] = None, indexes: Optional[Iterable[IndexModel]] = None, default_filter: Optional[Dict[str, Any]] = None, strict: bool = True, incremental_ids: bool = False):
        """Constructor

        Args:
            record_type (type[PacketBase]): record class(child of PacketBase)
            name (Optional[str], optional): database name of collection. If None - same as python name. Defaults to None.
            indexes (Optional[Iterable[IndexModel]], optional): list of indexes which will be ensured on connection. Defaults to None.
            default_filter (Optional[Dict[str, Any]], optional): _description_. Defaults to None.
            strict (bool, optional): _description_. Defaults to True.
            incremental_ids (bool, optional): _description_. Defaults to False.

        Raises:
            TypeError: _description_
        """
        assert issubclass(record_type, PacketBase), (record_type, type(record_type))
        self.indexes: List[IndexModel] = []
        if indexes:
            for index in indexes:
                if not isinstance(index, IndexModel):
                    raise TypeError(u'Index is not correct %s(%s)' % (index, type(index)))
                self.indexes.append(index)
        self.record_type = record_type
        self.name = name
        self.default_filter = default_filter or {}
        self.strict = strict
        self.incremental_ids = incremental_ids

    def clone(self) -> 'MongoCollectionField':
        return MongoCollectionField(self.record_type, self.name, self.indexes, self.default_filter, self.strict, self.incremental_ids)
