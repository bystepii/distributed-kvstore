from __future__ import annotations

import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from threading import Lock, Thread
from typing import List, Dict, Set

import grpc
import grpc.aio
from google.protobuf.empty_pb2 import Empty
from grpc import ServicerContext

from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")

LOOP = asyncio.get_event_loop()


def get_loop():
    global LOOP
    if LOOP is None:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(LOOP)
    return LOOP


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
        Return the leftmost character of the value associated with the key and remove it from the value.

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
        Return the rightmost character of the value associated with the key and remove it from the value.

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
        self.kv_store: Dict[int, str] = {}
        self.channels: Dict[str, grpc.aio.Channel] = {}
        self.stubs: Dict[str, KVStoreStub] = {}
        self.lock = Lock()

    def get(self, key: int) -> str | None:
        with self.lock:
            return self.kv_store.get(key)

    def l_pop(self, key: int) -> str | None:
        with self.lock:
            value = self.kv_store.get(key)
            if value is None:
                return None
            if len(value) == 0:
                return ""
            self.kv_store[key] = value[1:]
            return value[0]

    def r_pop(self, key: int) -> str | None:
        with self.lock:
            value = self.kv_store.get(key)
            if value is None:
                return None
            if len(value) == 0:
                return ""
            self.kv_store[key] = value[:-1]
            return value[-1]

    def put(self, key: int, value: str):
        with self.lock:
            self.kv_store[key] = value

    def append(self, key: int, value: str):
        with self.lock:
            self.kv_store[key] = self.kv_store.get(key, "") + value

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        with self.lock:
            loop = get_loop()

            # Save the channel to avoid creating a new channel every time
            if destination_server not in self.channels:
                self.channels[destination_server] = grpc.aio.insecure_channel(destination_server)
                self.stubs[destination_server] = KVStoreStub(self.channels[destination_server])

            # Transfer the keys and values in the range [lower_val, upper_val] to the destination server
            loop.run_until_complete(self.stubs[destination_server].Transfer(TransferRequest(keys_values=[
                KeyValue(key=key, value=value) for key, value in self.kv_store.items() if lower_val <= key <= upper_val
            ])))

            # Delete the keys and values in the range [lower_val, upper_val] from the local storage
            for key in list(self.kv_store.keys()):
                if lower_val <= key <= upper_val:
                    del self.kv_store[key]

    def transfer(self, keys_values: List[KeyValue]):
        with self.lock:
            for key_value in keys_values:
                self.kv_store[key_value.key] = key_value.value

    def add_replica(self, server: str):
        raise NotImplementedError

    def remove_replica(self, server: str):
        raise NotImplementedError


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        self.replicas: Set[str] = set()
        self.sample: Set[str] = set()
        self.dirty_keys: Set[int] = set()
        self._lock = Lock()
        self.thread = Thread(target=self._update_loop, daemon=True)

    def l_pop(self, key: int) -> str:
        with self._lock:
            if self.role == Role.MASTER:
                self.dirty_keys.add(key)
                get_loop().run_until_complete(asyncio.gather(*[
                    self.stubs[replica].LPop(GetRequest(key=key)) for replica in self.sample
                ]))
            return super().l_pop(key)

    def r_pop(self, key: int) -> str:
        with self._lock:
            if self.role == Role.MASTER:
                self.dirty_keys.add(key)
                get_loop().run_until_complete(asyncio.gather(*[
                    self.stubs[replica].RPop(GetRequest(key=key)) for replica in self.sample
                ]))
            return super().r_pop(key)

    def put(self, key: int, value: str):
        with self._lock:
            super().put(key, value)
            if self.role == Role.MASTER:
                self.dirty_keys.add(key)
                get_loop().run_until_complete(asyncio.gather(*[
                    self.stubs[replica].Put(PutRequest(key=key, value=value)) for replica in self.sample
                ]))

    def append(self, key: int, value: str):
        with self._lock:
            super().append(key, value)
            if self.role == Role.MASTER:
                self.dirty_keys.add(key)
                get_loop().run_until_complete(asyncio.gather(*[
                    self.stubs[replica].Append(AppendRequest(key=key, value=value)) for replica in self.sample
                ]))

    def add_replica(self, server: str):
        with self._lock:
            if self.role != Role.MASTER:
                raise TypeError("Only the master can add a replica")

            loop = get_loop()

            self.replicas.add(server)
            self.channels[server] = grpc.aio.insecure_channel(server)
            self.stubs[server] = KVStoreStub(self.channels[server])

            # initial transfer
            loop.run_until_complete(self.stubs[server].Transfer(TransferRequest(keys_values=[
                KeyValue(key=key, value=value) for key, value in self.kv_store.items()
            ])))

            # If the consistency level is reached, set the random sample
            if len(self.replicas) >= self.consistency_level and len(self.sample) == 0:
                self._set_sample()

    def remove_replica(self, server: str):
        with self._lock:
            if self.role != Role.MASTER:
                raise TypeError("Only the master can remove a replica")
            self.replicas.remove(server)
            del self.channels[server]
            del self.stubs[server]

    def _update_loop(self):
        while True:
            if len(self.replicas) > self.consistency_level and len(self.dirty_keys) > 0:
                with self._lock:
                    # Transfer the dirty keys to the replicas not in the sample
                    get_loop().run_until_complete(asyncio.gather(*[
                        self.stubs[replica].Transfer(TransferRequest(keys_values=[
                            KeyValue(key=key, value=value)
                            for key, value in self.kv_store.items() if key in self.dirty_keys
                        ])) for replica in (self.replicas - self.sample)
                    ]))

                    # Clear the dirty keys and set a new sample
                    self.dirty_keys.clear()
                    self._set_sample()
            time.sleep(EVENTUAL_CONSISTENCY_INTERVAL)

    def _set_sample(self):
        self.sample = set(random.sample(list(self.replicas), min(self.consistency_level, len(self.replicas))))

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role
        if role == Role.MASTER:
            self.thread.start()


class KVStorageServicer(KVStoreServicer):
    """
    gRPC servicer for KVStorageService

    :param service: KVStorageService implementation
    """

    def __init__(self, service: KVStorageService):
        self.storage_service = service

    def Get(self, request: GetRequest, context: ServicerContext) -> GetResponse:
        """
        Get the value associated with the key.
        :param request: GetRequest
        :param context: grpc.ServicerContext
        :return: GetResponse
        """
        return GetResponse(value=self.storage_service.get(request.key))

    def LPop(self, request: GetRequest, context: ServicerContext) -> GetResponse:
        """
        Remove and return the leftmost character of the value associated with the key.

        :param request: GetRequest
        :param context: grpc.ServicerContext
        :return: GetResponse
        """
        return GetResponse(value=self.storage_service.l_pop(request.key))

    def RPop(self, request: GetRequest, context: ServicerContext) -> GetResponse:
        """
        Remove and return the rightmost character of the value associated with the key.

        :param request: GetRequest
        :param context: grpc.ServicerContext
        :return: GetResponse
        """
        return GetResponse(value=self.storage_service.r_pop(request.key))

    def Put(self, request: PutRequest, context: ServicerContext) -> Empty:
        """
        Set the value associated with the key.

        :param request: PutRequest
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.storage_service.put(request.key, request.value)
        return Empty()

    def Append(self, request: AppendRequest, context: ServicerContext) -> Empty:
        """
        Concatenate the value to the end of the value associated with the key.

        :param request: AppendRequest
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.storage_service.append(request.key, request.value)
        return Empty()

    def Redistribute(self, request: RedistributeRequest, context: ServicerContext) -> Empty:
        """
        Redistribute the keys and values to the destination server.

        :param request: RedistributeRequest
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.storage_service.redistribute(request.destination_server, request.lower_val, request.upper_val)
        return Empty()

    def Transfer(self, request: TransferRequest, context: ServicerContext) -> Empty:
        """
        Transfer the keys and values to the destination server.

        :param request: TransferRequest
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.storage_service.transfer(list(request.keys_values))
        return Empty()

    def AddReplica(self, request: ServerRequest, context: ServicerContext) -> Empty:
        """
        Add a secondary replica to the replica master.

        :param request: ServerRequest
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.storage_service.add_replica(request.server)
        return Empty()

    def RemoveReplica(self, request: ServerRequest, context: ServicerContext) -> Empty:
        """
        Remove a secondary replica from the replica master.

        :param request: ServerRequest
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.storage_service.remove_replica(request.server)
        return Empty()
