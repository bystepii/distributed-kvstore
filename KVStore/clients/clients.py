from __future__ import annotations

from typing import Union, Dict
import grpc
import logging

from KVStore.kvstorage.kvstorage import KVStorageService
from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None


class SimpleClient(KVStorageService):

    """
    Simple client for key-value storage service
    """
    def __init__(self, kvstore_address: str):
        super().__init__()
        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)

    def get(self, key: int) -> str | None:
        return _get_return(self.stub.Get(GetRequest(key=key)))

    def l_pop(self, key: int) -> str | None:
        return _get_return(self.stub.LPop(GetRequest(key=key)))

    def r_pop(self, key: int) -> str | None:
        return _get_return(self.stub.RPop(GetRequest(key=key)))

    def put(self, key: int, value: str):
        self.stub.Put(PutRequest(key=key, value=value))

    def append(self, key: int, value: str):
        self.stub.Append(PutRequest(key=key, value=value))

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        raise NotImplementedError

    def transfer(self, keys_values: list):
        raise NotImplementedError

    def add_replica(self, server: str):
        raise NotImplementedError

    def remove_replica(self, server: str):
        raise NotImplementedError

    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        """
        To fill with your code
        """

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def put(self, key: int, value: str):
        """
        To fill with your code
        """

    def append(self, key: int, value: str):
        """
        To fill with your code
        """


class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def put(self, key: int, value: str):
        """
        To fill with your code
        """

    def append(self, key: int, value: str):
        """
        To fill with your code
        """
