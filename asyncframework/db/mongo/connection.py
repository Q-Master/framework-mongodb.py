# -*- coding: utf-8 -*-
from typing import Optional
from urllib.parse import quote_plus
from asyncframework.app.service import Service
from motor.motor_asyncio import AsyncIOMotorClient


__all__ = ['MongoConnection']


class MongoConnection(Service):
    """MongoDB service
    """
    __mongo_connection: AsyncIOMotorClient = None
    _uri: str

    def __init__(self, uri: str):
        """Constructor

        Args:
            uri (str): URI to connect to mongo instance
        """
        super().__init__()
        self._uri = uri

    @classmethod
    def from_host_port(cls, host: str, port: int, db: str, user: Optional[str] = None, password: Optional[str] = None, **additional_params) -> 'MongoConnection':
        """Construct connection from credentials

        Args:
            host (str): mongodb host
            port (int): mongodb port
            db (str): database name
            user (Optional[str], optional): username. Defaults to None.
            password (Optional[str], optional): password. Defaults to None.

        Returns:
            MongoConnection: the mongo connection service
        """
        if user and password:
            uri = f'mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}' 
        elif user and password is None:
            uri = f'mongodb://{quote_plus(user)}@{host}:{port}/{db}' 
        else:
            uri = f'mongodb://{host}:{port}/{db}'
        if additional_params:
            uri = '?'.join([uri, '&'.join('%s=%s' % (quote_plus(n), quote_plus(v)) for n, v in additional_params.items())])
        return cls(uri)

    async def __start__(self, *args, **kwargs):
        self.__mongo_connection = AsyncIOMotorClient(self._uri, io_loop=self.ioloop)
    
    async def __body__(self, *args, **kwargs):
        pass

    async def __stop__(self):
        self.__mongo_connection.close()

    def __getattr__(self, item):
        return getattr(self.__mongo_connection, item)
