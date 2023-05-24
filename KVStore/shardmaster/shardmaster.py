import logging
import random
from abc import ABC, abstractmethod
from math import ceil
from threading import Lock
from typing import Dict, List, Set

import grpc
from google.protobuf.empty_pb2 import Empty

from KVStore.protos import kv_store_shardmaster_pb2_grpc
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import *
from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD

logger = logging.getLogger(__name__)


class ShardMasterService(ABC):
    """
    Skeleton class for shard master service
    """

    def __init__(self):
        pass

    @abstractmethod
    def join(self, server: str):
        """
        Join a new server to the system.

        Shard master should redistribute the keys among the servers based on the new configuration.
        :param server: server address
        """
        pass

    @abstractmethod
    def leave(self, server: str):
        """
        Remove a server from the system

        Shard master should redistribute the keys among the servers based on the new configuration.
        :param server: server address
        :return:
        """
        pass

    @abstractmethod
    def query(self, key: int) -> str:
        """
        Query for the shard of a given key.

        :param key: key
        :return: server address
        """
        pass

    @abstractmethod
    def join_replica(self, server: str) -> Role:
        """
        Join a new storage replica to the system.

        Shard master should redistribute the keys among the servers based on the new configuration if the new replica
        becomes the replica master.
        :param server: server address
        :return: Role of the replica
        """
        pass

    @abstractmethod
    def query_replica(self, key: int, op: Operation) -> str:
        """
        Query for the replica of a given key.

        If the operation is read, return any replica.
        If the operation is write, return the replica master.
        :param key: key
        :param op: operation
        :return: server address
        """
        pass


class ShardMasterSimpleService(ShardMasterService):
    """
    Simple shard master service.
    """

    def __init__(self):
        super().__init__()
        self.servers: List[str] = []
        self.interval_size: int = KEYS_UPPER_THRESHOLD - KEYS_LOWER_THRESHOLD + 1
        self.channels: Dict[str, grpc.Channel] = {}
        self.lock = Lock()

    def join(self, server: str):
        with self.lock:
            self.channels[server] = grpc.insecure_channel(server)

            if len(self.servers) == 0:
                self.servers.append(server)
                return

            # Calculate new ranges
            num_servers = len(self.servers)
            new_num_servers = num_servers + 1
            new_interval_size = ceil((KEYS_UPPER_THRESHOLD - KEYS_LOWER_THRESHOLD + 1) / new_num_servers)

            # Add new server
            self.servers.append(server)

            # Redistribute keys
            for i in range(num_servers):
                start, end = i * self.interval_size, (i + 1) * self.interval_size
                new_start, new_end = i * new_interval_size, (i + 1) * new_interval_size
                KVStoreStub(self.channels[self.servers[i]]).Redistribute(
                    RedistributeRequest(destination_server=self.servers[i + 1], lower_val=new_end, upper_val=end)
                )

            self.interval_size = new_interval_size

    def leave(self, server: str):
        with self.lock:
            if server not in self.channels:
                return

            # special case: only one server left after removal
            if len(self.servers) == 2:
                self.servers.remove(server)
                self.interval_size = KEYS_UPPER_THRESHOLD - KEYS_LOWER_THRESHOLD + 1
                KVStoreStub(self.channels.pop(server)).Redistribute(
                    RedistributeRequest(
                        destination_server=self.servers[0],
                        lower_val=KEYS_LOWER_THRESHOLD,
                        upper_val=KEYS_UPPER_THRESHOLD + 1
                    )
                )
                return

            # special case: no servers
            if len(self.servers) == 1:
                self.servers.remove(server)
                return

            # Calculate new ranges
            num_servers = len(self.servers)
            new_num_servers = num_servers - 1
            new_interval_size = ceil((KEYS_UPPER_THRESHOLD - KEYS_LOWER_THRESHOLD + 1) / new_num_servers)

            index = self.servers.index(server)  # get index of server to be removed

            # Redistribute keys

            # Case 1: server is the first server
            if index == 0:
                # iterate through all servers except the last one
                for i in range(0, num_servers - 1):
                    start, end = i * self.interval_size, (i + 1) * self.interval_size
                    new_start, new_end = i * new_interval_size, (i + 1) * new_interval_size
                    KVStoreStub(self.channels[self.servers[i]]).Redistribute(
                        RedistributeRequest(destination_server=self.servers[i + 1], lower_val=new_start, upper_val=end)
                    )
            # Case 2: server is the last server
            elif index == num_servers - 1:
                # iterate backwards through all servers except the first one
                for i in reversed(range(1, num_servers)):
                    start, end = (i - 1) * self.interval_size, i * self.interval_size
                    new_start, new_end = (i - 1) * new_interval_size, i * new_interval_size
                    KVStoreStub(self.channels[self.servers[i]]).Redistribute(
                        RedistributeRequest(destination_server=self.servers[i - 1], lower_val=new_start, upper_val=end)
                    )
            # Case 3: server is in the middle
            else:
                # iterate forwards through all servers except the last one
                for i in range(index, num_servers - 1):
                    # transfer only half of the keys to the next server forwards
                    start, end = i * self.interval_size, (i + 1) * self.interval_size
                    new_start, new_end = i * new_interval_size, (i + 1) * new_interval_size
                    KVStoreStub(self.channels[self.servers[i]]).Redistribute(
                        RedistributeRequest(destination_server=self.servers[i + 1], lower_val=new_start, upper_val=end)
                    )
                # iterate backwards through all servers except the first one
                for i in reversed(range(1, index + 1)):
                    # transfer the other half of the keys to the next server backwards
                    start, end = (i - 1) * self.interval_size, i * self.interval_size
                    new_start, new_end = (i - 1) * new_interval_size, i * new_interval_size
                    KVStoreStub(self.channels[self.servers[i]]).Redistribute(
                        RedistributeRequest(destination_server=self.servers[i - 1], lower_val=new_start, upper_val=end)
                    )

            # Remove server
            self.servers.remove(server)
            self.interval_size = new_interval_size

    def query(self, key: int) -> str:
        if key < KEYS_LOWER_THRESHOLD or key > KEYS_UPPER_THRESHOLD:
            raise ValueError(f"Key {key} out of range")
        with self.lock:
            return self.servers[(key - KEYS_LOWER_THRESHOLD) // self.interval_size]

    def join_replica(self, server: str) -> Role:
        raise NotImplementedError

    def query_replica(self, key: int, op: Operation) -> str:
        raise NotImplementedError


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        self.number_of_shards = number_of_shards
        self.master_to_replicas: Dict[str, Set[str]] = {}
        self.replica_to_master: Dict[str, str] = {}
        self._lock = Lock()

    def leave(self, server: str):
        with self._lock:
            # if the server is a master, remove it from the list of servers
            if server in self.servers:
                super().leave(server)
                return

            # if the server is a replica, remove it from the list of replicas
            if server in self.replica_to_master:
                master = self.replica_to_master[server]
                self.master_to_replicas[master].remove(server)
                del self.replica_to_master[server]

                # notify the master of the removal of the replica
                KVStoreStub(self.channels[master]).RemoveReplica(
                    ServerRequest(server=server)
                )

    def join_replica(self, server: str) -> Role:
        with self._lock:
            # for the first number_of_shards servers, assign them to be masters
            if len(self.servers) < self.number_of_shards:
                super().join(server)
                self.master_to_replicas[server] = set()
                return Role.MASTER

            # for the rest of the servers, assign them to be replicas
            # add the replica to the replica master with the least number of replicas
            least = min(self.master_to_replicas, key=lambda x: len(self.master_to_replicas[x]))
            self.master_to_replicas[least].add(server)
            self.replica_to_master[server] = least

            # notify the replica master of the new replica
            KVStoreStub(self.channels[least]).AddReplica(
                ServerRequest(server=server)
            )

            return Role.REPLICA

    def query_replica(self, key: int, op: Operation) -> str:
        with self._lock:
            # get the shard index
            shard = self.query(key)

            # if the operation is a read operation, return random replica
            if op == Operation.GET:
                replica = random.sample(sorted(self.master_to_replicas[shard]) + [shard], 1)[0]
                if replica:
                    return replica
            return shard


class ShardMasterServicer(kv_store_shardmaster_pb2_grpc.ShardMasterServicer):
    """
    gRPC servicer for shard master service

    :param shard_master_service: shard master service
    """

    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service

    def Join(self, request: JoinRequest, context: grpc.ServicerContext) -> Empty:
        """
        Storage server join request. The shard master should redistribute the keys and values based on the new
        number of shards.

        :param request: join request
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.shard_master_service.join(request.server)
        return Empty()

    def Leave(self, request: LeaveRequest, context: grpc.ServicerContext) -> Empty:
        """
        Storage server leave request. The shard master should redistribute the keys and values based on the new
        number of shards.

        :param request: leave request
        :param context: grpc.ServicerContext
        :return: Empty
        """
        self.shard_master_service.leave(request.server)
        return Empty()

    def Query(self, request: QueryRequest, context: grpc.ServicerContext) -> QueryResponse:
        """
        Query for the shard of a given key.

        :param request: query request
        :param context: grpc.ServicerContext
        :return: QueryResponse with the shard address
        """
        return QueryResponse(server=self.shard_master_service.query(request.key))

    def JoinReplica(self, request: JoinRequest, context: grpc.ServicerContext) -> JoinReplicaResponse:
        """
        Replica join request. The shard master should redistribute the keys and values if the new replica is
        replica master.

        :param request: join request
        :param context: grpc.ServicerContext
        :return: JoinReplicaResponse with the replica role
        """
        return JoinReplicaResponse(role=self.shard_master_service.join_replica(request.server))

    def QueryReplica(self, request: QueryReplicaRequest, context: grpc.ServicerContext) -> QueryResponse:
        """
        Query for the replica of a given key.

        If the operation is read, return any secondary replica.
        If the operation is write, return the replica master.

        :param request: query request
        :param context: grpc.ServicerContext
        :return: QueryReplicaResponse with the replica address
        """
        return QueryResponse(server=self.shard_master_service.query_replica(request.key, request.operation))
