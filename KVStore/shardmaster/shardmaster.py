import logging
from abc import ABC, abstractmethod

import grpc
from google.protobuf.empty_pb2 import Empty

from KVStore.protos import kv_store_shardmaster_pb2_grpc
from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import *

logger = logging.getLogger(__name__)


class ShardMasterService(ABC):

    """
    Skeleton class for shard master service
    """
    def __init__(self):
        pass

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
        """
        To fill with your code
        """

    def join(self, server: str):
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def query(self, key: int) -> str:
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        raise NotImplementedError

    def query_replica(self, key: int, op: Operation) -> str:
        raise NotImplementedError


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


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
