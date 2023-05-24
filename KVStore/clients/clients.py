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


class ShardClient(KVStorageService):
    def __init__(self, shard_master_address: str):
        super().__init__()
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        self.channels: Dict[str, grpc.Channel] = {}

    def get(self, key: int) -> Union[str, None]:
        return _get_return(self._query_shard(key).Get(GetRequest(key=key)))

    def l_pop(self, key: int) -> Union[str, None]:
        return _get_return(self._query_shard(key).LPop(GetRequest(key=key)))

    def r_pop(self, key: int) -> Union[str, None]:
        return _get_return(self._query_shard(key).RPop(GetRequest(key=key)))

    def put(self, key: int, value: str):
        return self._query_shard(key).Put(PutRequest(key=key, value=value))

    def append(self, key: int, value: str):
        return self._query_shard(key).Append(PutRequest(key=key, value=value))

    def _query_shard(self, key: int) -> KVStoreStub:
        server = self.stub.Query(QueryRequest(key=key)).server
        channel = self.channels.get(server, grpc.insecure_channel(server))
        self.channels[server] = channel
        stub = KVStoreStub(channel)
        return stub

    def stop(self):
        self.channel.close()
        for channel in self.channels.values():
            channel.close()

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        raise NotImplementedError

    def transfer(self, keys_values: list):
        raise NotImplementedError

    def add_replica(self, server: str):
        raise NotImplementedError

    def remove_replica(self, server: str):
        raise NotImplementedError


class ShardReplicaClient(ShardClient):

    def __init__(self, shard_master_address: str):
        super().__init__(shard_master_address)
        self.channels: Dict[str, grpc.Channel] = {}

    def get(self, key: int) -> str | None:
        return _get_return(self._query_replica(key, Operation.GET).Get(GetRequest(key=key)))

    def l_pop(self, key: int) -> str | None:
        return _get_return(self._query_replica(key, Operation.L_POP).LPop(GetRequest(key=key)))

    def r_pop(self, key: int) -> str | None:
        return _get_return(self._query_replica(key, Operation.R_POP).RPop(GetRequest(key=key)))

    def put(self, key: int, value: str):
        return self._query_replica(key, Operation.PUT).Put(PutRequest(key=key, value=value))

    def append(self, key: int, value: str):
        return self._query_replica(key, Operation.APPEND).Append(PutRequest(key=key, value=value))

    def _query_replica(self, key: int, operation: Operation) -> KVStoreStub:
        server = self.stub.QueryReplica(QueryReplicaRequest(key=key, operation=operation)).server
        self.channels[server] = grpc.insecure_channel(server)
        return KVStoreStub(self.channels[server])
