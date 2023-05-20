from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Union, List

from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer
from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService(ABC):

    """
    Skeleton class for key-value storage service
    """
    def __init__(self):
        pass

    @abstractmethod
    def get(self, key: int) -> str | None:
        """
        Get the value associated with the key
        :param key: key
        :return: value associated with the key, or None if the key does not exist
        """
        pass

    @abstractmethod
    def l_pop(self, key: int) -> str | None:
        """
        Return the rightmost character of the value associated with the key and remove it from the value.

        If the last character is removed, the value becomes an empty string ("").
        If l_pop is called on an empty string, return an empty string ("").
        If the key does not exist, return None.
        :param key: key
        :return: rightmost character of the value associated with the key, or None if the key does not exist
        """
        pass

    @abstractmethod
    def r_pop(self, key: int) -> str | None:
        """
        Return the leftmost character of the value associated with the key and remove it from the value.

        If the last character is removed, the value becomes an empty string ("").
        If r_pop is called on an empty string, return an empty string ("").
        If the key does not exist, return None.
        :param key:
        :return: leftmost character of the value associated with the key, or None if the key does not exist
        """
        pass

    @abstractmethod
    def put(self, key: int, value: str):
        """
        Put the value associated with the key. If the key already exists, overwrite the value.
        :param key: key
        :param value: value associated with the key
        """
        pass

    @abstractmethod
    def append(self, key: int, value: str):
        """
        Concatenate the value to the end of the value associated with the key. If the key does not exist, create a new
        key-value pair.
        :param key: key
        :param value: value to concatenate
        """
        pass

    @abstractmethod
    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        """
        Redistribute the keys and values in the range [lower_val, upper_val] to the destination server.

        This method is called by the shard master to a storage server when a new server is added to the system in order
        to redistribute the shards.
        :param destination_server: hostname of the destination server
        :param lower_val: lower bound of the range
        :param upper_val: upper bound of the range
        """
        pass

    @abstractmethod
    def transfer(self, keys_values: list):
        """
        Transfer the keys and values to the destination server.

        This method is called from one storage server to another storage server.
        :param keys_values: list of key-value pairs
        """
        pass

    @abstractmethod
    def add_replica(self, server: str):
        """
        Add a secondary replica to the replica master.
        :param server: hostname of the secondary replica
        """
        pass

    @abstractmethod
    def remove_replica(self, server: str):
        """
        Remove a secondary replica from the replica master.
        :param server: hostname of the secondary replica
        """
        pass


class KVStorageSimpleService(KVStorageService):

    """
    Simple implementation of KVStorageService using a dictionary
    """
    def __init__(self):
        super().__init__()
        self.kv_store = dict()

    def get(self, key: int) -> str | None:
        return self.kv_store.get(key)

    def l_pop(self, key: int) -> str | None:
        value = self.kv_store.get(key)
        if value is None:
            return None
        if len(value) == 0:
            return ""
        self.kv_store[key] = value[:-1]
        return value[-1]

    def r_pop(self, key: int) -> str | None:
        value = self.kv_store.get(key)
        if value is None:
            return None
        if len(value) == 0:
            return ""
        self.kv_store[key] = value[1:]
        return value[0]

    def put(self, key: int, value: str):
        self.kv_store[key] = value

    def append(self, key: int, value: str):
        self.kv_store[key] = self.kv_store.get(key, "") + value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        raise NotImplementedError

    def transfer(self, keys_values: List[KeyValue]):
        raise NotImplementedError

    def add_replica(self, server: str):
        raise NotImplementedError

    def remove_replica(self, server: str):
        raise NotImplementedError


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
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

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:
        """
        To fill with your code
        """

    def LPop(self, request: GetRequest, context) -> GetResponse:
        """
        To fill with your code
        """

    def RPop(self, request: GetRequest, context) -> GetResponse:
        """
        To fill with your code
        """

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """
