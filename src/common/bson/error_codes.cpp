/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

// Modified by Keonwoo Oh (koh3@umd.edu) from June 6th to 16th, 2023

// #include "mongo/platform/basic.h"

#include "common/bson/error_codes.h"

#include "common/bson/static_assert.h"
#include "common/bson/assert_util.h"
#include "common/bson/str.h"


namespace mongo {

namespace {
// You can think of this namespace as a compile-time map<ErrorCodes::Error, ErrorExtraInfoParser*>.
namespace parsers {
ErrorExtraInfo::Parser* MultipleErrorsOccurred = nullptr;
ErrorExtraInfo::Parser* ShutdownInProgress = nullptr;
ErrorExtraInfo::Parser* DocumentValidationFailure = nullptr;
ErrorExtraInfo::Parser* StaleEpoch = nullptr;
ErrorExtraInfo::Parser* CommandOnShardedViewNotSupportedOnMongod = nullptr;
ErrorExtraInfo::Parser* CannotImplicitlyCreateCollection = nullptr;
ErrorExtraInfo::Parser* ForTestingErrorExtraInfo = nullptr;
ErrorExtraInfo::Parser* StaleDbVersion = nullptr;
ErrorExtraInfo::Parser* JSInterpreterFailureWithStack = nullptr;
ErrorExtraInfo::Parser* WouldChangeOwningShard = nullptr;
ErrorExtraInfo::Parser* ForTestingErrorExtraInfoWithExtraInfoInNamespace = nullptr;
ErrorExtraInfo::Parser* ShardInvalidatedForTargeting = nullptr;
ErrorExtraInfo::Parser* ForTestingOptionalErrorExtraInfo = nullptr;
ErrorExtraInfo::Parser* TenantMigrationConflict = nullptr;
ErrorExtraInfo::Parser* ShardCannotRefreshDueToLocksHeld = nullptr;
ErrorExtraInfo::Parser* ChangeStreamInvalidated = nullptr;
ErrorExtraInfo::Parser* ChangeStreamTopologyChange = nullptr;
ErrorExtraInfo::Parser* ChangeStreamStartAfterInvalidate = nullptr;
ErrorExtraInfo::Parser* NonRetryableTenantMigrationConflict = nullptr;
ErrorExtraInfo::Parser* TxnRetryCounterTooOld = nullptr;
ErrorExtraInfo::Parser* CannotConvertIndexToUnique = nullptr;
ErrorExtraInfo::Parser* CollectionUUIDMismatch = nullptr;
ErrorExtraInfo::Parser* ReshardingCoordinatorServiceConflictingOperationInProgress = nullptr;
ErrorExtraInfo::Parser* RemoteCommandExecutionError = nullptr;
ErrorExtraInfo::Parser* DuplicateKey = nullptr;
ErrorExtraInfo::Parser* StaleConfig = nullptr;
}  // namespace parsers
}  // namespace


MONGO_STATIC_ASSERT(sizeof(ErrorCodes::Error) == sizeof(int));

std::string ErrorCodes::errorString(Error err) {
    switch (err) {
        case OK:
            return "OK";
        case InternalError:
            return "InternalError";
        case BadValue:
            return "BadValue";
        case OBSOLETE_DuplicateKey:
            return "OBSOLETE_DuplicateKey";
        case NoSuchKey:
            return "NoSuchKey";
        case GraphContainsCycle:
            return "GraphContainsCycle";
        case HostUnreachable:
            return "HostUnreachable";
        case HostNotFound:
            return "HostNotFound";
        case UnknownError:
            return "UnknownError";
        case FailedToParse:
            return "FailedToParse";
        case CannotMutateObject:
            return "CannotMutateObject";
        case UserNotFound:
            return "UserNotFound";
        case UnsupportedFormat:
            return "UnsupportedFormat";
        case Unauthorized:
            return "Unauthorized";
        case TypeMismatch:
            return "TypeMismatch";
        case Overflow:
            return "Overflow";
        case InvalidLength:
            return "InvalidLength";
        case ProtocolError:
            return "ProtocolError";
        case AuthenticationFailed:
            return "AuthenticationFailed";
        case CannotReuseObject:
            return "CannotReuseObject";
        case IllegalOperation:
            return "IllegalOperation";
        case EmptyArrayOperation:
            return "EmptyArrayOperation";
        case InvalidBSON:
            return "InvalidBSON";
        case AlreadyInitialized:
            return "AlreadyInitialized";
        case LockTimeout:
            return "LockTimeout";
        case RemoteValidationError:
            return "RemoteValidationError";
        case NamespaceNotFound:
            return "NamespaceNotFound";
        case IndexNotFound:
            return "IndexNotFound";
        case PathNotViable:
            return "PathNotViable";
        case NonExistentPath:
            return "NonExistentPath";
        case InvalidPath:
            return "InvalidPath";
        case RoleNotFound:
            return "RoleNotFound";
        case RolesNotRelated:
            return "RolesNotRelated";
        case PrivilegeNotFound:
            return "PrivilegeNotFound";
        case CannotBackfillArray:
            return "CannotBackfillArray";
        case UserModificationFailed:
            return "UserModificationFailed";
        case RemoteChangeDetected:
            return "RemoteChangeDetected";
        case FileRenameFailed:
            return "FileRenameFailed";
        case FileNotOpen:
            return "FileNotOpen";
        case FileStreamFailed:
            return "FileStreamFailed";
        case ConflictingUpdateOperators:
            return "ConflictingUpdateOperators";
        case FileAlreadyOpen:
            return "FileAlreadyOpen";
        case LogWriteFailed:
            return "LogWriteFailed";
        case CursorNotFound:
            return "CursorNotFound";
        case UserDataInconsistent:
            return "UserDataInconsistent";
        case LockBusy:
            return "LockBusy";
        case NoMatchingDocument:
            return "NoMatchingDocument";
        case NamespaceExists:
            return "NamespaceExists";
        case InvalidRoleModification:
            return "InvalidRoleModification";
        case MaxTimeMSExpired:
            return "MaxTimeMSExpired";
        case ManualInterventionRequired:
            return "ManualInterventionRequired";
        case DollarPrefixedFieldName:
            return "DollarPrefixedFieldName";
        case InvalidIdField:
            return "InvalidIdField";
        case NotSingleValueField:
            return "NotSingleValueField";
        case InvalidDBRef:
            return "InvalidDBRef";
        case EmptyFieldName:
            return "EmptyFieldName";
        case DottedFieldName:
            return "DottedFieldName";
        case RoleModificationFailed:
            return "RoleModificationFailed";
        case CommandNotFound:
            return "CommandNotFound";
        case OBSOLETE_DatabaseNotFound:
            return "OBSOLETE_DatabaseNotFound";
        case ShardKeyNotFound:
            return "ShardKeyNotFound";
        case OplogOperationUnsupported:
            return "OplogOperationUnsupported";
        case OBSOLETE_StaleShardVersion:
            return "OBSOLETE_StaleShardVersion";
        case WriteConcernFailed:
            return "WriteConcernFailed";
        case MultipleErrorsOccurred:
            return "MultipleErrorsOccurred";
        case ImmutableField:
            return "ImmutableField";
        case CannotCreateIndex:
            return "CannotCreateIndex";
        case IndexAlreadyExists:
            return "IndexAlreadyExists";
        case AuthSchemaIncompatible:
            return "AuthSchemaIncompatible";
        case ShardNotFound:
            return "ShardNotFound";
        case ReplicaSetNotFound:
            return "ReplicaSetNotFound";
        case InvalidOptions:
            return "InvalidOptions";
        case InvalidNamespace:
            return "InvalidNamespace";
        case NodeNotFound:
            return "NodeNotFound";
        case WriteConcernLegacyOK:
            return "WriteConcernLegacyOK";
        case NoReplicationEnabled:
            return "NoReplicationEnabled";
        case OperationIncomplete:
            return "OperationIncomplete";
        case CommandResultSchemaViolation:
            return "CommandResultSchemaViolation";
        case UnknownReplWriteConcern:
            return "UnknownReplWriteConcern";
        case RoleDataInconsistent:
            return "RoleDataInconsistent";
        case NoMatchParseContext:
            return "NoMatchParseContext";
        case NoProgressMade:
            return "NoProgressMade";
        case RemoteResultsUnavailable:
            return "RemoteResultsUnavailable";
        case OBSOLETE_DuplicateKeyValue:
            return "OBSOLETE_DuplicateKeyValue";
        case IndexOptionsConflict:
            return "IndexOptionsConflict";
        case IndexKeySpecsConflict:
            return "IndexKeySpecsConflict";
        case CannotSplit:
            return "CannotSplit";
        case OBSOLETE_SplitFailed:
            return "OBSOLETE_SplitFailed";
        case NetworkTimeout:
            return "NetworkTimeout";
        case CallbackCanceled:
            return "CallbackCanceled";
        case ShutdownInProgress:
            return "ShutdownInProgress";
        case SecondaryAheadOfPrimary:
            return "SecondaryAheadOfPrimary";
        case InvalidReplicaSetConfig:
            return "InvalidReplicaSetConfig";
        case NotYetInitialized:
            return "NotYetInitialized";
        case NotSecondary:
            return "NotSecondary";
        case OperationFailed:
            return "OperationFailed";
        case NoProjectionFound:
            return "NoProjectionFound";
        case DBPathInUse:
            return "DBPathInUse";
        case UnsatisfiableWriteConcern:
            return "UnsatisfiableWriteConcern";
        case OutdatedClient:
            return "OutdatedClient";
        case IncompatibleAuditMetadata:
            return "IncompatibleAuditMetadata";
        case NewReplicaSetConfigurationIncompatible:
            return "NewReplicaSetConfigurationIncompatible";
        case NodeNotElectable:
            return "NodeNotElectable";
        case IncompatibleShardingMetadata:
            return "IncompatibleShardingMetadata";
        case DistributedClockSkewed:
            return "DistributedClockSkewed";
        case LockFailed:
            return "LockFailed";
        case InconsistentReplicaSetNames:
            return "InconsistentReplicaSetNames";
        case ConfigurationInProgress:
            return "ConfigurationInProgress";
        case CannotInitializeNodeWithData:
            return "CannotInitializeNodeWithData";
        case NotExactValueField:
            return "NotExactValueField";
        case WriteConflict:
            return "WriteConflict";
        case InitialSyncFailure:
            return "InitialSyncFailure";
        case InitialSyncOplogSourceMissing:
            return "InitialSyncOplogSourceMissing";
        case CommandNotSupported:
            return "CommandNotSupported";
        case DocTooLargeForCapped:
            return "DocTooLargeForCapped";
        case ConflictingOperationInProgress:
            return "ConflictingOperationInProgress";
        case NamespaceNotSharded:
            return "NamespaceNotSharded";
        case InvalidSyncSource:
            return "InvalidSyncSource";
        case OplogStartMissing:
            return "OplogStartMissing";
        case DocumentValidationFailure:
            return "DocumentValidationFailure";
        case OBSOLETE_ReadAfterOptimeTimeout:
            return "OBSOLETE_ReadAfterOptimeTimeout";
        case NotAReplicaSet:
            return "NotAReplicaSet";
        case IncompatibleElectionProtocol:
            return "IncompatibleElectionProtocol";
        case CommandFailed:
            return "CommandFailed";
        case RPCProtocolNegotiationFailed:
            return "RPCProtocolNegotiationFailed";
        case UnrecoverableRollbackError:
            return "UnrecoverableRollbackError";
        case LockNotFound:
            return "LockNotFound";
        case LockStateChangeFailed:
            return "LockStateChangeFailed";
        case SymbolNotFound:
            return "SymbolNotFound";
        case OBSOLETE_ConfigServersInconsistent:
            return "OBSOLETE_ConfigServersInconsistent";
        case FailedToSatisfyReadPreference:
            return "FailedToSatisfyReadPreference";
        case ReadConcernMajorityNotAvailableYet:
            return "ReadConcernMajorityNotAvailableYet";
        case StaleTerm:
            return "StaleTerm";
        case CappedPositionLost:
            return "CappedPositionLost";
        case IncompatibleShardingConfigVersion:
            return "IncompatibleShardingConfigVersion";
        case RemoteOplogStale:
            return "RemoteOplogStale";
        case JSInterpreterFailure:
            return "JSInterpreterFailure";
        case InvalidSSLConfiguration:
            return "InvalidSSLConfiguration";
        case SSLHandshakeFailed:
            return "SSLHandshakeFailed";
        case JSUncatchableError:
            return "JSUncatchableError";
        case CursorInUse:
            return "CursorInUse";
        case IncompatibleCatalogManager:
            return "IncompatibleCatalogManager";
        case PooledConnectionsDropped:
            return "PooledConnectionsDropped";
        case ExceededMemoryLimit:
            return "ExceededMemoryLimit";
        case ZLibError:
            return "ZLibError";
        case ReadConcernMajorityNotEnabled:
            return "ReadConcernMajorityNotEnabled";
        case NoConfigPrimary:
            return "NoConfigPrimary";
        case StaleEpoch:
            return "StaleEpoch";
        case OperationCannotBeBatched:
            return "OperationCannotBeBatched";
        case OplogOutOfOrder:
            return "OplogOutOfOrder";
        case ChunkTooBig:
            return "ChunkTooBig";
        case InconsistentShardIdentity:
            return "InconsistentShardIdentity";
        case CannotApplyOplogWhilePrimary:
            return "CannotApplyOplogWhilePrimary";
        case OBSOLETE_NeedsDocumentMove:
            return "OBSOLETE_NeedsDocumentMove";
        case CanRepairToDowngrade:
            return "CanRepairToDowngrade";
        case MustUpgrade:
            return "MustUpgrade";
        case DurationOverflow:
            return "DurationOverflow";
        case MaxStalenessOutOfRange:
            return "MaxStalenessOutOfRange";
        case IncompatibleCollationVersion:
            return "IncompatibleCollationVersion";
        case CollectionIsEmpty:
            return "CollectionIsEmpty";
        case ZoneStillInUse:
            return "ZoneStillInUse";
        case InitialSyncActive:
            return "InitialSyncActive";
        case ViewDepthLimitExceeded:
            return "ViewDepthLimitExceeded";
        case CommandNotSupportedOnView:
            return "CommandNotSupportedOnView";
        case OptionNotSupportedOnView:
            return "OptionNotSupportedOnView";
        case InvalidPipelineOperator:
            return "InvalidPipelineOperator";
        case CommandOnShardedViewNotSupportedOnMongod:
            return "CommandOnShardedViewNotSupportedOnMongod";
        case TooManyMatchingDocuments:
            return "TooManyMatchingDocuments";
        case CannotIndexParallelArrays:
            return "CannotIndexParallelArrays";
        case TransportSessionClosed:
            return "TransportSessionClosed";
        case TransportSessionNotFound:
            return "TransportSessionNotFound";
        case TransportSessionUnknown:
            return "TransportSessionUnknown";
        case QueryPlanKilled:
            return "QueryPlanKilled";
        case FileOpenFailed:
            return "FileOpenFailed";
        case ZoneNotFound:
            return "ZoneNotFound";
        case RangeOverlapConflict:
            return "RangeOverlapConflict";
        case WindowsPdhError:
            return "WindowsPdhError";
        case BadPerfCounterPath:
            return "BadPerfCounterPath";
        case AmbiguousIndexKeyPattern:
            return "AmbiguousIndexKeyPattern";
        case InvalidViewDefinition:
            return "InvalidViewDefinition";
        case ClientMetadataMissingField:
            return "ClientMetadataMissingField";
        case ClientMetadataAppNameTooLarge:
            return "ClientMetadataAppNameTooLarge";
        case ClientMetadataDocumentTooLarge:
            return "ClientMetadataDocumentTooLarge";
        case ClientMetadataCannotBeMutated:
            return "ClientMetadataCannotBeMutated";
        case LinearizableReadConcernError:
            return "LinearizableReadConcernError";
        case IncompatibleServerVersion:
            return "IncompatibleServerVersion";
        case PrimarySteppedDown:
            return "PrimarySteppedDown";
        case MasterSlaveConnectionFailure:
            return "MasterSlaveConnectionFailure";
        case OBSOLETE_BalancerLostDistributedLock:
            return "OBSOLETE_BalancerLostDistributedLock";
        case FailPointEnabled:
            return "FailPointEnabled";
        case NoShardingEnabled:
            return "NoShardingEnabled";
        case BalancerInterrupted:
            return "BalancerInterrupted";
        case ViewPipelineMaxSizeExceeded:
            return "ViewPipelineMaxSizeExceeded";
        case InvalidIndexSpecificationOption:
            return "InvalidIndexSpecificationOption";
        case OBSOLETE_ReceivedOpReplyMessage:
            return "OBSOLETE_ReceivedOpReplyMessage";
        case ReplicaSetMonitorRemoved:
            return "ReplicaSetMonitorRemoved";
        case ChunkRangeCleanupPending:
            return "ChunkRangeCleanupPending";
        case CannotBuildIndexKeys:
            return "CannotBuildIndexKeys";
        case NetworkInterfaceExceededTimeLimit:
            return "NetworkInterfaceExceededTimeLimit";
        case ShardingStateNotInitialized:
            return "ShardingStateNotInitialized";
        case TimeProofMismatch:
            return "TimeProofMismatch";
        case ClusterTimeFailsRateLimiter:
            return "ClusterTimeFailsRateLimiter";
        case NoSuchSession:
            return "NoSuchSession";
        case InvalidUUID:
            return "InvalidUUID";
        case TooManyLocks:
            return "TooManyLocks";
        case StaleClusterTime:
            return "StaleClusterTime";
        case CannotVerifyAndSignLogicalTime:
            return "CannotVerifyAndSignLogicalTime";
        case KeyNotFound:
            return "KeyNotFound";
        case IncompatibleRollbackAlgorithm:
            return "IncompatibleRollbackAlgorithm";
        case DuplicateSession:
            return "DuplicateSession";
        case AuthenticationRestrictionUnmet:
            return "AuthenticationRestrictionUnmet";
        case DatabaseDropPending:
            return "DatabaseDropPending";
        case ElectionInProgress:
            return "ElectionInProgress";
        case IncompleteTransactionHistory:
            return "IncompleteTransactionHistory";
        case UpdateOperationFailed:
            return "UpdateOperationFailed";
        case FTDCPathNotSet:
            return "FTDCPathNotSet";
        case FTDCPathAlreadySet:
            return "FTDCPathAlreadySet";
        case IndexModified:
            return "IndexModified";
        case CloseChangeStream:
            return "CloseChangeStream";
        case IllegalOpMsgFlag:
            return "IllegalOpMsgFlag";
        case QueryFeatureNotAllowed:
            return "QueryFeatureNotAllowed";
        case TransactionTooOld:
            return "TransactionTooOld";
        case AtomicityFailure:
            return "AtomicityFailure";
        case CannotImplicitlyCreateCollection:
            return "CannotImplicitlyCreateCollection";
        case SessionTransferIncomplete:
            return "SessionTransferIncomplete";
        case MustDowngrade:
            return "MustDowngrade";
        case DNSHostNotFound:
            return "DNSHostNotFound";
        case DNSProtocolError:
            return "DNSProtocolError";
        case MaxSubPipelineDepthExceeded:
            return "MaxSubPipelineDepthExceeded";
        case TooManyDocumentSequences:
            return "TooManyDocumentSequences";
        case RetryChangeStream:
            return "RetryChangeStream";
        case InternalErrorNotSupported:
            return "InternalErrorNotSupported";
        case ForTestingErrorExtraInfo:
            return "ForTestingErrorExtraInfo";
        case CursorKilled:
            return "CursorKilled";
        case NotImplemented:
            return "NotImplemented";
        case SnapshotTooOld:
            return "SnapshotTooOld";
        case DNSRecordTypeMismatch:
            return "DNSRecordTypeMismatch";
        case ConversionFailure:
            return "ConversionFailure";
        case CannotCreateCollection:
            return "CannotCreateCollection";
        case IncompatibleWithUpgradedServer:
            return "IncompatibleWithUpgradedServer";
        case NOT_YET_AVAILABLE_TransactionAborted:
            return "NOT_YET_AVAILABLE_TransactionAborted";
        case BrokenPromise:
            return "BrokenPromise";
        case SnapshotUnavailable:
            return "SnapshotUnavailable";
        case ProducerConsumerQueueBatchTooLarge:
            return "ProducerConsumerQueueBatchTooLarge";
        case ProducerConsumerQueueEndClosed:
            return "ProducerConsumerQueueEndClosed";
        case StaleDbVersion:
            return "StaleDbVersion";
        case StaleChunkHistory:
            return "StaleChunkHistory";
        case NoSuchTransaction:
            return "NoSuchTransaction";
        case ReentrancyNotAllowed:
            return "ReentrancyNotAllowed";
        case FreeMonHttpInFlight:
            return "FreeMonHttpInFlight";
        case FreeMonHttpTemporaryFailure:
            return "FreeMonHttpTemporaryFailure";
        case FreeMonHttpPermanentFailure:
            return "FreeMonHttpPermanentFailure";
        case TransactionCommitted:
            return "TransactionCommitted";
        case TransactionTooLarge:
            return "TransactionTooLarge";
        case UnknownFeatureCompatibilityVersion:
            return "UnknownFeatureCompatibilityVersion";
        case KeyedExecutorRetry:
            return "KeyedExecutorRetry";
        case InvalidResumeToken:
            return "InvalidResumeToken";
        case TooManyLogicalSessions:
            return "TooManyLogicalSessions";
        case ExceededTimeLimit:
            return "ExceededTimeLimit";
        case OperationNotSupportedInTransaction:
            return "OperationNotSupportedInTransaction";
        case TooManyFilesOpen:
            return "TooManyFilesOpen";
        case OrphanedRangeCleanUpFailed:
            return "OrphanedRangeCleanUpFailed";
        case FailPointSetFailed:
            return "FailPointSetFailed";
        case PreparedTransactionInProgress:
            return "PreparedTransactionInProgress";
        case CannotBackup:
            return "CannotBackup";
        case DataModifiedByRepair:
            return "DataModifiedByRepair";
        case RepairedReplicaSetNode:
            return "RepairedReplicaSetNode";
        case JSInterpreterFailureWithStack:
            return "JSInterpreterFailureWithStack";
        case MigrationConflict:
            return "MigrationConflict";
        case ProducerConsumerQueueProducerQueueDepthExceeded:
            return "ProducerConsumerQueueProducerQueueDepthExceeded";
        case ProducerConsumerQueueConsumed:
            return "ProducerConsumerQueueConsumed";
        case ExchangePassthrough:
            return "ExchangePassthrough";
        case IndexBuildAborted:
            return "IndexBuildAborted";
        case AlarmAlreadyFulfilled:
            return "AlarmAlreadyFulfilled";
        case UnsatisfiableCommitQuorum:
            return "UnsatisfiableCommitQuorum";
        case ClientDisconnect:
            return "ClientDisconnect";
        case ChangeStreamFatalError:
            return "ChangeStreamFatalError";
        case TransactionCoordinatorSteppingDown:
            return "TransactionCoordinatorSteppingDown";
        case TransactionCoordinatorReachedAbortDecision:
            return "TransactionCoordinatorReachedAbortDecision";
        case WouldChangeOwningShard:
            return "WouldChangeOwningShard";
        case ForTestingErrorExtraInfoWithExtraInfoInNamespace:
            return "ForTestingErrorExtraInfoWithExtraInfoInNamespace";
        case IndexBuildAlreadyInProgress:
            return "IndexBuildAlreadyInProgress";
        case ChangeStreamHistoryLost:
            return "ChangeStreamHistoryLost";
        case TransactionCoordinatorDeadlineTaskCanceled:
            return "TransactionCoordinatorDeadlineTaskCanceled";
        case ChecksumMismatch:
            return "ChecksumMismatch";
        case WaitForMajorityServiceEarlierOpTimeAvailable:
            return "WaitForMajorityServiceEarlierOpTimeAvailable";
        case TransactionExceededLifetimeLimitSeconds:
            return "TransactionExceededLifetimeLimitSeconds";
        case NoQueryExecutionPlans:
            return "NoQueryExecutionPlans";
        case QueryExceededMemoryLimitNoDiskUseAllowed:
            return "QueryExceededMemoryLimitNoDiskUseAllowed";
        case InvalidSeedList:
            return "InvalidSeedList";
        case InvalidTopologyType:
            return "InvalidTopologyType";
        case InvalidHeartBeatFrequency:
            return "InvalidHeartBeatFrequency";
        case TopologySetNameRequired:
            return "TopologySetNameRequired";
        case HierarchicalAcquisitionLevelViolation:
            return "HierarchicalAcquisitionLevelViolation";
        case InvalidServerType:
            return "InvalidServerType";
        case OCSPCertificateStatusRevoked:
            return "OCSPCertificateStatusRevoked";
        case RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist:
            return "RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist";
        case DataCorruptionDetected:
            return "DataCorruptionDetected";
        case OCSPCertificateStatusUnknown:
            return "OCSPCertificateStatusUnknown";
        case SplitHorizonChange:
            return "SplitHorizonChange";
        case ShardInvalidatedForTargeting:
            return "ShardInvalidatedForTargeting";
        case ReadThroughCacheLookupCanceled:
            return "ReadThroughCacheLookupCanceled";
        case RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist:
            return "RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist";
        case CurrentConfigNotCommittedYet:
            return "CurrentConfigNotCommittedYet";
        case ExhaustCommandFinished:
            return "ExhaustCommandFinished";
        case PeriodicJobIsStopped:
            return "PeriodicJobIsStopped";
        case TransactionCoordinatorCanceled:
            return "TransactionCoordinatorCanceled";
        case OperationIsKilledAndDelisted:
            return "OperationIsKilledAndDelisted";
        case ResumableRangeDeleterDisabled:
            return "ResumableRangeDeleterDisabled";
        case ObjectIsBusy:
            return "ObjectIsBusy";
        case TooStaleToSyncFromSource:
            return "TooStaleToSyncFromSource";
        case QueryTrialRunCompleted:
            return "QueryTrialRunCompleted";
        case ConnectionPoolExpired:
            return "ConnectionPoolExpired";
        case ForTestingOptionalErrorExtraInfo:
            return "ForTestingOptionalErrorExtraInfo";
        case MovePrimaryInProgress:
            return "MovePrimaryInProgress";
        case TenantMigrationConflict:
            return "TenantMigrationConflict";
        case TenantMigrationCommitted:
            return "TenantMigrationCommitted";
        case APIVersionError:
            return "APIVersionError";
        case APIStrictError:
            return "APIStrictError";
        case APIDeprecationError:
            return "APIDeprecationError";
        case TenantMigrationAborted:
            return "TenantMigrationAborted";
        case OplogQueryMinTsMissing:
            return "OplogQueryMinTsMissing";
        case NoSuchTenantMigration:
            return "NoSuchTenantMigration";
        case TenantMigrationAccessBlockerShuttingDown:
            return "TenantMigrationAccessBlockerShuttingDown";
        case TenantMigrationInProgress:
            return "TenantMigrationInProgress";
        case SkipCommandExecution:
            return "SkipCommandExecution";
        case FailedToRunWithReplyBuilder:
            return "FailedToRunWithReplyBuilder";
        case CannotDowngrade:
            return "CannotDowngrade";
        case ServiceExecutorInShutdown:
            return "ServiceExecutorInShutdown";
        case MechanismUnavailable:
            return "MechanismUnavailable";
        case TenantMigrationForgotten:
            return "TenantMigrationForgotten";
        case TimeseriesBucketCleared:
            return "TimeseriesBucketCleared";
        case AuthenticationAbandoned:
            return "AuthenticationAbandoned";
        case ReshardCollectionInProgress:
            return "ReshardCollectionInProgress";
        case NoSuchReshardCollection:
            return "NoSuchReshardCollection";
        case ReshardCollectionCommitted:
            return "ReshardCollectionCommitted";
        case ReshardCollectionAborted:
            return "ReshardCollectionAborted";
        case ReshardingCriticalSectionTimeout:
            return "ReshardingCriticalSectionTimeout";
        case ShardCannotRefreshDueToLocksHeld:
            return "ShardCannotRefreshDueToLocksHeld";
        case AuditingNotEnabled:
            return "AuditingNotEnabled";
        case RuntimeAuditConfigurationNotEnabled:
            return "RuntimeAuditConfigurationNotEnabled";
        case ChangeStreamInvalidated:
            return "ChangeStreamInvalidated";
        case APIMismatchError:
            return "APIMismatchError";
        case ChangeStreamTopologyChange:
            return "ChangeStreamTopologyChange";
        case KeyPatternShorterThanBound:
            return "KeyPatternShorterThanBound";
        case ReshardCollectionTruncatedError:
            return "ReshardCollectionTruncatedError";
        case ChangeStreamStartAfterInvalidate:
            return "ChangeStreamStartAfterInvalidate";
        case UnsupportedOpQueryCommand:
            return "UnsupportedOpQueryCommand";
        case NonRetryableTenantMigrationConflict:
            return "NonRetryableTenantMigrationConflict";
        case LoadBalancerSupportMismatch:
            return "LoadBalancerSupportMismatch";
        case InterruptedDueToStorageChange:
            return "InterruptedDueToStorageChange";
        case TxnRetryCounterTooOld:
            return "TxnRetryCounterTooOld";
        case InvalidBSONType:
            return "InvalidBSONType";
        case InternalTransactionNotSupported:
            return "InternalTransactionNotSupported";
        case CannotConvertIndexToUnique:
            return "CannotConvertIndexToUnique";
        case ShardVersionRefreshCanceled:
            return "ShardVersionRefreshCanceled";
        case CollectionUUIDMismatch:
            return "CollectionUUIDMismatch";
        case FutureAlreadyRetrieved:
            return "FutureAlreadyRetrieved";
        case RetryableTransactionInProgress:
            return "RetryableTransactionInProgress";
        case TemporarilyUnavailable:
            return "TemporarilyUnavailable";
        case WouldChangeOwningShardDeletedNoDocument:
            return "WouldChangeOwningShardDeletedNoDocument";
        case FLECompactionPlaceholder:
            return "FLECompactionPlaceholder";
        case FLETransactionAbort:
            return "FLETransactionAbort";
        case CannotDropShardKeyIndex:
            return "CannotDropShardKeyIndex";
        case UserWritesBlocked:
            return "UserWritesBlocked";
        case CloseConnectionForShutdownCommand:
            return "CloseConnectionForShutdownCommand";
        case InternalTransactionsExhaustiveFindHasMore:
            return "InternalTransactionsExhaustiveFindHasMore";
        case TransactionAPIMustRetryTransaction:
            return "TransactionAPIMustRetryTransaction";
        case TransactionAPIMustRetryCommit:
            return "TransactionAPIMustRetryCommit";
        case ChangeStreamNotEnabled:
            return "ChangeStreamNotEnabled";
        case FLEMaxTagLimitExceeded:
            return "FLEMaxTagLimitExceeded";
        case NonConformantBSON:
            return "NonConformantBSON";
        case DatabaseMetadataRefreshCanceled:
            return "DatabaseMetadataRefreshCanceled";
        case RequestAlreadyFulfilled:
            return "RequestAlreadyFulfilled";
        case ReshardingCoordinatorServiceConflictingOperationInProgress:
            return "ReshardingCoordinatorServiceConflictingOperationInProgress";
        case RemoteCommandExecutionError:
            return "RemoteCommandExecutionError";
        case CollectionIsEmptyLocally:
            return "CollectionIsEmptyLocally";
        case ConnectionError:
            return "ConnectionError";
        case ConflictingServerlessOperation:
            return "ConflictingServerlessOperation";
        case SocketException:
            return "SocketException";
        case OBSOLETE_RecvStaleConfig:
            return "OBSOLETE_RecvStaleConfig";
        case CannotGrowDocumentInCappedNamespace:
            return "CannotGrowDocumentInCappedNamespace";
        case LegacyNotPrimary:
            return "LegacyNotPrimary";
        case NotWritablePrimary:
            return "NotWritablePrimary";
        case BSONObjectTooLarge:
            return "BSONObjectTooLarge";
        case DuplicateKey:
            return "DuplicateKey";
        case InterruptedAtShutdown:
            return "InterruptedAtShutdown";
        case Interrupted:
            return "Interrupted";
        case InterruptedDueToReplStateChange:
            return "InterruptedDueToReplStateChange";
        case BackgroundOperationInProgressForDatabase:
            return "BackgroundOperationInProgressForDatabase";
        case BackgroundOperationInProgressForNamespace:
            return "BackgroundOperationInProgressForNamespace";
        case OBSOLETE_PrepareConfigsFailed:
            return "OBSOLETE_PrepareConfigsFailed";
        case MergeStageNoMatchingDocument:
            return "MergeStageNoMatchingDocument";
        case DatabaseDifferCase:
            return "DatabaseDifferCase";
        case StaleConfig:
            return "StaleConfig";
        case NotPrimaryNoSecondaryOk:
            return "NotPrimaryNoSecondaryOk";
        case NotPrimaryOrSecondary:
            return "NotPrimaryOrSecondary";
        case OutOfDiskSpace:
            return "OutOfDiskSpace";
        case OBSOLETE_KeyTooLong:
            return "OBSOLETE_KeyTooLong";
        case ClientMarkedKilled:
            return "ClientMarkedKilled";
        case NotARetryableWriteCommand:
            return "NotARetryableWriteCommand";
        case BackupCursorOpenConflictWithCheckpoint:
            return "BackupCursorOpenConflictWithCheckpoint";
        case ConfigServerUnreachable:
            return "ConfigServerUnreachable";
        default:
            return str::stream() << "Location" << int(err);
    }
}

ErrorCodes::Error ErrorCodes::fromString(StringData name) {
    if (name == "OK"_sd)
        return OK;
    if (name == "InternalError"_sd)
        return InternalError;
    if (name == "BadValue"_sd)
        return BadValue;
    if (name == "OBSOLETE_DuplicateKey"_sd)
        return OBSOLETE_DuplicateKey;
    if (name == "NoSuchKey"_sd)
        return NoSuchKey;
    if (name == "GraphContainsCycle"_sd)
        return GraphContainsCycle;
    if (name == "HostUnreachable"_sd)
        return HostUnreachable;
    if (name == "HostNotFound"_sd)
        return HostNotFound;
    if (name == "UnknownError"_sd)
        return UnknownError;
    if (name == "FailedToParse"_sd)
        return FailedToParse;
    if (name == "CannotMutateObject"_sd)
        return CannotMutateObject;
    if (name == "UserNotFound"_sd)
        return UserNotFound;
    if (name == "UnsupportedFormat"_sd)
        return UnsupportedFormat;
    if (name == "Unauthorized"_sd)
        return Unauthorized;
    if (name == "TypeMismatch"_sd)
        return TypeMismatch;
    if (name == "Overflow"_sd)
        return Overflow;
    if (name == "InvalidLength"_sd)
        return InvalidLength;
    if (name == "ProtocolError"_sd)
        return ProtocolError;
    if (name == "AuthenticationFailed"_sd)
        return AuthenticationFailed;
    if (name == "CannotReuseObject"_sd)
        return CannotReuseObject;
    if (name == "IllegalOperation"_sd)
        return IllegalOperation;
    if (name == "EmptyArrayOperation"_sd)
        return EmptyArrayOperation;
    if (name == "InvalidBSON"_sd)
        return InvalidBSON;
    if (name == "AlreadyInitialized"_sd)
        return AlreadyInitialized;
    if (name == "LockTimeout"_sd)
        return LockTimeout;
    if (name == "RemoteValidationError"_sd)
        return RemoteValidationError;
    if (name == "NamespaceNotFound"_sd)
        return NamespaceNotFound;
    if (name == "IndexNotFound"_sd)
        return IndexNotFound;
    if (name == "PathNotViable"_sd)
        return PathNotViable;
    if (name == "NonExistentPath"_sd)
        return NonExistentPath;
    if (name == "InvalidPath"_sd)
        return InvalidPath;
    if (name == "RoleNotFound"_sd)
        return RoleNotFound;
    if (name == "RolesNotRelated"_sd)
        return RolesNotRelated;
    if (name == "PrivilegeNotFound"_sd)
        return PrivilegeNotFound;
    if (name == "CannotBackfillArray"_sd)
        return CannotBackfillArray;
    if (name == "UserModificationFailed"_sd)
        return UserModificationFailed;
    if (name == "RemoteChangeDetected"_sd)
        return RemoteChangeDetected;
    if (name == "FileRenameFailed"_sd)
        return FileRenameFailed;
    if (name == "FileNotOpen"_sd)
        return FileNotOpen;
    if (name == "FileStreamFailed"_sd)
        return FileStreamFailed;
    if (name == "ConflictingUpdateOperators"_sd)
        return ConflictingUpdateOperators;
    if (name == "FileAlreadyOpen"_sd)
        return FileAlreadyOpen;
    if (name == "LogWriteFailed"_sd)
        return LogWriteFailed;
    if (name == "CursorNotFound"_sd)
        return CursorNotFound;
    if (name == "UserDataInconsistent"_sd)
        return UserDataInconsistent;
    if (name == "LockBusy"_sd)
        return LockBusy;
    if (name == "NoMatchingDocument"_sd)
        return NoMatchingDocument;
    if (name == "NamespaceExists"_sd)
        return NamespaceExists;
    if (name == "InvalidRoleModification"_sd)
        return InvalidRoleModification;
    if (name == "MaxTimeMSExpired"_sd)
        return MaxTimeMSExpired;
    if (name == "ManualInterventionRequired"_sd)
        return ManualInterventionRequired;
    if (name == "DollarPrefixedFieldName"_sd)
        return DollarPrefixedFieldName;
    if (name == "InvalidIdField"_sd)
        return InvalidIdField;
    if (name == "NotSingleValueField"_sd)
        return NotSingleValueField;
    if (name == "InvalidDBRef"_sd)
        return InvalidDBRef;
    if (name == "EmptyFieldName"_sd)
        return EmptyFieldName;
    if (name == "DottedFieldName"_sd)
        return DottedFieldName;
    if (name == "RoleModificationFailed"_sd)
        return RoleModificationFailed;
    if (name == "CommandNotFound"_sd)
        return CommandNotFound;
    if (name == "OBSOLETE_DatabaseNotFound"_sd)
        return OBSOLETE_DatabaseNotFound;
    if (name == "ShardKeyNotFound"_sd)
        return ShardKeyNotFound;
    if (name == "OplogOperationUnsupported"_sd)
        return OplogOperationUnsupported;
    if (name == "OBSOLETE_StaleShardVersion"_sd)
        return OBSOLETE_StaleShardVersion;
    if (name == "WriteConcernFailed"_sd)
        return WriteConcernFailed;
    if (name == "MultipleErrorsOccurred"_sd)
        return MultipleErrorsOccurred;
    if (name == "ImmutableField"_sd)
        return ImmutableField;
    if (name == "CannotCreateIndex"_sd)
        return CannotCreateIndex;
    if (name == "IndexAlreadyExists"_sd)
        return IndexAlreadyExists;
    if (name == "AuthSchemaIncompatible"_sd)
        return AuthSchemaIncompatible;
    if (name == "ShardNotFound"_sd)
        return ShardNotFound;
    if (name == "ReplicaSetNotFound"_sd)
        return ReplicaSetNotFound;
    if (name == "InvalidOptions"_sd)
        return InvalidOptions;
    if (name == "InvalidNamespace"_sd)
        return InvalidNamespace;
    if (name == "NodeNotFound"_sd)
        return NodeNotFound;
    if (name == "WriteConcernLegacyOK"_sd)
        return WriteConcernLegacyOK;
    if (name == "NoReplicationEnabled"_sd)
        return NoReplicationEnabled;
    if (name == "OperationIncomplete"_sd)
        return OperationIncomplete;
    if (name == "CommandResultSchemaViolation"_sd)
        return CommandResultSchemaViolation;
    if (name == "UnknownReplWriteConcern"_sd)
        return UnknownReplWriteConcern;
    if (name == "RoleDataInconsistent"_sd)
        return RoleDataInconsistent;
    if (name == "NoMatchParseContext"_sd)
        return NoMatchParseContext;
    if (name == "NoProgressMade"_sd)
        return NoProgressMade;
    if (name == "RemoteResultsUnavailable"_sd)
        return RemoteResultsUnavailable;
    if (name == "OBSOLETE_DuplicateKeyValue"_sd)
        return OBSOLETE_DuplicateKeyValue;
    if (name == "IndexOptionsConflict"_sd)
        return IndexOptionsConflict;
    if (name == "IndexKeySpecsConflict"_sd)
        return IndexKeySpecsConflict;
    if (name == "CannotSplit"_sd)
        return CannotSplit;
    if (name == "OBSOLETE_SplitFailed"_sd)
        return OBSOLETE_SplitFailed;
    if (name == "NetworkTimeout"_sd)
        return NetworkTimeout;
    if (name == "CallbackCanceled"_sd)
        return CallbackCanceled;
    if (name == "ShutdownInProgress"_sd)
        return ShutdownInProgress;
    if (name == "SecondaryAheadOfPrimary"_sd)
        return SecondaryAheadOfPrimary;
    if (name == "InvalidReplicaSetConfig"_sd)
        return InvalidReplicaSetConfig;
    if (name == "NotYetInitialized"_sd)
        return NotYetInitialized;
    if (name == "NotSecondary"_sd)
        return NotSecondary;
    if (name == "OperationFailed"_sd)
        return OperationFailed;
    if (name == "NoProjectionFound"_sd)
        return NoProjectionFound;
    if (name == "DBPathInUse"_sd)
        return DBPathInUse;
    if (name == "UnsatisfiableWriteConcern"_sd)
        return UnsatisfiableWriteConcern;
    if (name == "OutdatedClient"_sd)
        return OutdatedClient;
    if (name == "IncompatibleAuditMetadata"_sd)
        return IncompatibleAuditMetadata;
    if (name == "NewReplicaSetConfigurationIncompatible"_sd)
        return NewReplicaSetConfigurationIncompatible;
    if (name == "NodeNotElectable"_sd)
        return NodeNotElectable;
    if (name == "IncompatibleShardingMetadata"_sd)
        return IncompatibleShardingMetadata;
    if (name == "DistributedClockSkewed"_sd)
        return DistributedClockSkewed;
    if (name == "LockFailed"_sd)
        return LockFailed;
    if (name == "InconsistentReplicaSetNames"_sd)
        return InconsistentReplicaSetNames;
    if (name == "ConfigurationInProgress"_sd)
        return ConfigurationInProgress;
    if (name == "CannotInitializeNodeWithData"_sd)
        return CannotInitializeNodeWithData;
    if (name == "NotExactValueField"_sd)
        return NotExactValueField;
    if (name == "WriteConflict"_sd)
        return WriteConflict;
    if (name == "InitialSyncFailure"_sd)
        return InitialSyncFailure;
    if (name == "InitialSyncOplogSourceMissing"_sd)
        return InitialSyncOplogSourceMissing;
    if (name == "CommandNotSupported"_sd)
        return CommandNotSupported;
    if (name == "DocTooLargeForCapped"_sd)
        return DocTooLargeForCapped;
    if (name == "ConflictingOperationInProgress"_sd)
        return ConflictingOperationInProgress;
    if (name == "NamespaceNotSharded"_sd)
        return NamespaceNotSharded;
    if (name == "InvalidSyncSource"_sd)
        return InvalidSyncSource;
    if (name == "OplogStartMissing"_sd)
        return OplogStartMissing;
    if (name == "DocumentValidationFailure"_sd)
        return DocumentValidationFailure;
    if (name == "OBSOLETE_ReadAfterOptimeTimeout"_sd)
        return OBSOLETE_ReadAfterOptimeTimeout;
    if (name == "NotAReplicaSet"_sd)
        return NotAReplicaSet;
    if (name == "IncompatibleElectionProtocol"_sd)
        return IncompatibleElectionProtocol;
    if (name == "CommandFailed"_sd)
        return CommandFailed;
    if (name == "RPCProtocolNegotiationFailed"_sd)
        return RPCProtocolNegotiationFailed;
    if (name == "UnrecoverableRollbackError"_sd)
        return UnrecoverableRollbackError;
    if (name == "LockNotFound"_sd)
        return LockNotFound;
    if (name == "LockStateChangeFailed"_sd)
        return LockStateChangeFailed;
    if (name == "SymbolNotFound"_sd)
        return SymbolNotFound;
    if (name == "OBSOLETE_ConfigServersInconsistent"_sd)
        return OBSOLETE_ConfigServersInconsistent;
    if (name == "FailedToSatisfyReadPreference"_sd)
        return FailedToSatisfyReadPreference;
    if (name == "ReadConcernMajorityNotAvailableYet"_sd)
        return ReadConcernMajorityNotAvailableYet;
    if (name == "StaleTerm"_sd)
        return StaleTerm;
    if (name == "CappedPositionLost"_sd)
        return CappedPositionLost;
    if (name == "IncompatibleShardingConfigVersion"_sd)
        return IncompatibleShardingConfigVersion;
    if (name == "RemoteOplogStale"_sd)
        return RemoteOplogStale;
    if (name == "JSInterpreterFailure"_sd)
        return JSInterpreterFailure;
    if (name == "InvalidSSLConfiguration"_sd)
        return InvalidSSLConfiguration;
    if (name == "SSLHandshakeFailed"_sd)
        return SSLHandshakeFailed;
    if (name == "JSUncatchableError"_sd)
        return JSUncatchableError;
    if (name == "CursorInUse"_sd)
        return CursorInUse;
    if (name == "IncompatibleCatalogManager"_sd)
        return IncompatibleCatalogManager;
    if (name == "PooledConnectionsDropped"_sd)
        return PooledConnectionsDropped;
    if (name == "ExceededMemoryLimit"_sd)
        return ExceededMemoryLimit;
    if (name == "ZLibError"_sd)
        return ZLibError;
    if (name == "ReadConcernMajorityNotEnabled"_sd)
        return ReadConcernMajorityNotEnabled;
    if (name == "NoConfigPrimary"_sd)
        return NoConfigPrimary;
    if (name == "StaleEpoch"_sd)
        return StaleEpoch;
    if (name == "OperationCannotBeBatched"_sd)
        return OperationCannotBeBatched;
    if (name == "OplogOutOfOrder"_sd)
        return OplogOutOfOrder;
    if (name == "ChunkTooBig"_sd)
        return ChunkTooBig;
    if (name == "InconsistentShardIdentity"_sd)
        return InconsistentShardIdentity;
    if (name == "CannotApplyOplogWhilePrimary"_sd)
        return CannotApplyOplogWhilePrimary;
    if (name == "OBSOLETE_NeedsDocumentMove"_sd)
        return OBSOLETE_NeedsDocumentMove;
    if (name == "CanRepairToDowngrade"_sd)
        return CanRepairToDowngrade;
    if (name == "MustUpgrade"_sd)
        return MustUpgrade;
    if (name == "DurationOverflow"_sd)
        return DurationOverflow;
    if (name == "MaxStalenessOutOfRange"_sd)
        return MaxStalenessOutOfRange;
    if (name == "IncompatibleCollationVersion"_sd)
        return IncompatibleCollationVersion;
    if (name == "CollectionIsEmpty"_sd)
        return CollectionIsEmpty;
    if (name == "ZoneStillInUse"_sd)
        return ZoneStillInUse;
    if (name == "InitialSyncActive"_sd)
        return InitialSyncActive;
    if (name == "ViewDepthLimitExceeded"_sd)
        return ViewDepthLimitExceeded;
    if (name == "CommandNotSupportedOnView"_sd)
        return CommandNotSupportedOnView;
    if (name == "OptionNotSupportedOnView"_sd)
        return OptionNotSupportedOnView;
    if (name == "InvalidPipelineOperator"_sd)
        return InvalidPipelineOperator;
    if (name == "CommandOnShardedViewNotSupportedOnMongod"_sd)
        return CommandOnShardedViewNotSupportedOnMongod;
    if (name == "TooManyMatchingDocuments"_sd)
        return TooManyMatchingDocuments;
    if (name == "CannotIndexParallelArrays"_sd)
        return CannotIndexParallelArrays;
    if (name == "TransportSessionClosed"_sd)
        return TransportSessionClosed;
    if (name == "TransportSessionNotFound"_sd)
        return TransportSessionNotFound;
    if (name == "TransportSessionUnknown"_sd)
        return TransportSessionUnknown;
    if (name == "QueryPlanKilled"_sd)
        return QueryPlanKilled;
    if (name == "FileOpenFailed"_sd)
        return FileOpenFailed;
    if (name == "ZoneNotFound"_sd)
        return ZoneNotFound;
    if (name == "RangeOverlapConflict"_sd)
        return RangeOverlapConflict;
    if (name == "WindowsPdhError"_sd)
        return WindowsPdhError;
    if (name == "BadPerfCounterPath"_sd)
        return BadPerfCounterPath;
    if (name == "AmbiguousIndexKeyPattern"_sd)
        return AmbiguousIndexKeyPattern;
    if (name == "InvalidViewDefinition"_sd)
        return InvalidViewDefinition;
    if (name == "ClientMetadataMissingField"_sd)
        return ClientMetadataMissingField;
    if (name == "ClientMetadataAppNameTooLarge"_sd)
        return ClientMetadataAppNameTooLarge;
    if (name == "ClientMetadataDocumentTooLarge"_sd)
        return ClientMetadataDocumentTooLarge;
    if (name == "ClientMetadataCannotBeMutated"_sd)
        return ClientMetadataCannotBeMutated;
    if (name == "LinearizableReadConcernError"_sd)
        return LinearizableReadConcernError;
    if (name == "IncompatibleServerVersion"_sd)
        return IncompatibleServerVersion;
    if (name == "PrimarySteppedDown"_sd)
        return PrimarySteppedDown;
    if (name == "MasterSlaveConnectionFailure"_sd)
        return MasterSlaveConnectionFailure;
    if (name == "OBSOLETE_BalancerLostDistributedLock"_sd)
        return OBSOLETE_BalancerLostDistributedLock;
    if (name == "FailPointEnabled"_sd)
        return FailPointEnabled;
    if (name == "NoShardingEnabled"_sd)
        return NoShardingEnabled;
    if (name == "BalancerInterrupted"_sd)
        return BalancerInterrupted;
    if (name == "ViewPipelineMaxSizeExceeded"_sd)
        return ViewPipelineMaxSizeExceeded;
    if (name == "InvalidIndexSpecificationOption"_sd)
        return InvalidIndexSpecificationOption;
    if (name == "OBSOLETE_ReceivedOpReplyMessage"_sd)
        return OBSOLETE_ReceivedOpReplyMessage;
    if (name == "ReplicaSetMonitorRemoved"_sd)
        return ReplicaSetMonitorRemoved;
    if (name == "ChunkRangeCleanupPending"_sd)
        return ChunkRangeCleanupPending;
    if (name == "CannotBuildIndexKeys"_sd)
        return CannotBuildIndexKeys;
    if (name == "NetworkInterfaceExceededTimeLimit"_sd)
        return NetworkInterfaceExceededTimeLimit;
    if (name == "ShardingStateNotInitialized"_sd)
        return ShardingStateNotInitialized;
    if (name == "TimeProofMismatch"_sd)
        return TimeProofMismatch;
    if (name == "ClusterTimeFailsRateLimiter"_sd)
        return ClusterTimeFailsRateLimiter;
    if (name == "NoSuchSession"_sd)
        return NoSuchSession;
    if (name == "InvalidUUID"_sd)
        return InvalidUUID;
    if (name == "TooManyLocks"_sd)
        return TooManyLocks;
    if (name == "StaleClusterTime"_sd)
        return StaleClusterTime;
    if (name == "CannotVerifyAndSignLogicalTime"_sd)
        return CannotVerifyAndSignLogicalTime;
    if (name == "KeyNotFound"_sd)
        return KeyNotFound;
    if (name == "IncompatibleRollbackAlgorithm"_sd)
        return IncompatibleRollbackAlgorithm;
    if (name == "DuplicateSession"_sd)
        return DuplicateSession;
    if (name == "AuthenticationRestrictionUnmet"_sd)
        return AuthenticationRestrictionUnmet;
    if (name == "DatabaseDropPending"_sd)
        return DatabaseDropPending;
    if (name == "ElectionInProgress"_sd)
        return ElectionInProgress;
    if (name == "IncompleteTransactionHistory"_sd)
        return IncompleteTransactionHistory;
    if (name == "UpdateOperationFailed"_sd)
        return UpdateOperationFailed;
    if (name == "FTDCPathNotSet"_sd)
        return FTDCPathNotSet;
    if (name == "FTDCPathAlreadySet"_sd)
        return FTDCPathAlreadySet;
    if (name == "IndexModified"_sd)
        return IndexModified;
    if (name == "CloseChangeStream"_sd)
        return CloseChangeStream;
    if (name == "IllegalOpMsgFlag"_sd)
        return IllegalOpMsgFlag;
    if (name == "QueryFeatureNotAllowed"_sd)
        return QueryFeatureNotAllowed;
    if (name == "TransactionTooOld"_sd)
        return TransactionTooOld;
    if (name == "AtomicityFailure"_sd)
        return AtomicityFailure;
    if (name == "CannotImplicitlyCreateCollection"_sd)
        return CannotImplicitlyCreateCollection;
    if (name == "SessionTransferIncomplete"_sd)
        return SessionTransferIncomplete;
    if (name == "MustDowngrade"_sd)
        return MustDowngrade;
    if (name == "DNSHostNotFound"_sd)
        return DNSHostNotFound;
    if (name == "DNSProtocolError"_sd)
        return DNSProtocolError;
    if (name == "MaxSubPipelineDepthExceeded"_sd)
        return MaxSubPipelineDepthExceeded;
    if (name == "TooManyDocumentSequences"_sd)
        return TooManyDocumentSequences;
    if (name == "RetryChangeStream"_sd)
        return RetryChangeStream;
    if (name == "InternalErrorNotSupported"_sd)
        return InternalErrorNotSupported;
    if (name == "ForTestingErrorExtraInfo"_sd)
        return ForTestingErrorExtraInfo;
    if (name == "CursorKilled"_sd)
        return CursorKilled;
    if (name == "NotImplemented"_sd)
        return NotImplemented;
    if (name == "SnapshotTooOld"_sd)
        return SnapshotTooOld;
    if (name == "DNSRecordTypeMismatch"_sd)
        return DNSRecordTypeMismatch;
    if (name == "ConversionFailure"_sd)
        return ConversionFailure;
    if (name == "CannotCreateCollection"_sd)
        return CannotCreateCollection;
    if (name == "IncompatibleWithUpgradedServer"_sd)
        return IncompatibleWithUpgradedServer;
    if (name == "NOT_YET_AVAILABLE_TransactionAborted"_sd)
        return NOT_YET_AVAILABLE_TransactionAborted;
    if (name == "BrokenPromise"_sd)
        return BrokenPromise;
    if (name == "SnapshotUnavailable"_sd)
        return SnapshotUnavailable;
    if (name == "ProducerConsumerQueueBatchTooLarge"_sd)
        return ProducerConsumerQueueBatchTooLarge;
    if (name == "ProducerConsumerQueueEndClosed"_sd)
        return ProducerConsumerQueueEndClosed;
    if (name == "StaleDbVersion"_sd)
        return StaleDbVersion;
    if (name == "StaleChunkHistory"_sd)
        return StaleChunkHistory;
    if (name == "NoSuchTransaction"_sd)
        return NoSuchTransaction;
    if (name == "ReentrancyNotAllowed"_sd)
        return ReentrancyNotAllowed;
    if (name == "FreeMonHttpInFlight"_sd)
        return FreeMonHttpInFlight;
    if (name == "FreeMonHttpTemporaryFailure"_sd)
        return FreeMonHttpTemporaryFailure;
    if (name == "FreeMonHttpPermanentFailure"_sd)
        return FreeMonHttpPermanentFailure;
    if (name == "TransactionCommitted"_sd)
        return TransactionCommitted;
    if (name == "TransactionTooLarge"_sd)
        return TransactionTooLarge;
    if (name == "UnknownFeatureCompatibilityVersion"_sd)
        return UnknownFeatureCompatibilityVersion;
    if (name == "KeyedExecutorRetry"_sd)
        return KeyedExecutorRetry;
    if (name == "InvalidResumeToken"_sd)
        return InvalidResumeToken;
    if (name == "TooManyLogicalSessions"_sd)
        return TooManyLogicalSessions;
    if (name == "ExceededTimeLimit"_sd)
        return ExceededTimeLimit;
    if (name == "OperationNotSupportedInTransaction"_sd)
        return OperationNotSupportedInTransaction;
    if (name == "TooManyFilesOpen"_sd)
        return TooManyFilesOpen;
    if (name == "OrphanedRangeCleanUpFailed"_sd)
        return OrphanedRangeCleanUpFailed;
    if (name == "FailPointSetFailed"_sd)
        return FailPointSetFailed;
    if (name == "PreparedTransactionInProgress"_sd)
        return PreparedTransactionInProgress;
    if (name == "CannotBackup"_sd)
        return CannotBackup;
    if (name == "DataModifiedByRepair"_sd)
        return DataModifiedByRepair;
    if (name == "RepairedReplicaSetNode"_sd)
        return RepairedReplicaSetNode;
    if (name == "JSInterpreterFailureWithStack"_sd)
        return JSInterpreterFailureWithStack;
    if (name == "MigrationConflict"_sd)
        return MigrationConflict;
    if (name == "ProducerConsumerQueueProducerQueueDepthExceeded"_sd)
        return ProducerConsumerQueueProducerQueueDepthExceeded;
    if (name == "ProducerConsumerQueueConsumed"_sd)
        return ProducerConsumerQueueConsumed;
    if (name == "ExchangePassthrough"_sd)
        return ExchangePassthrough;
    if (name == "IndexBuildAborted"_sd)
        return IndexBuildAborted;
    if (name == "AlarmAlreadyFulfilled"_sd)
        return AlarmAlreadyFulfilled;
    if (name == "UnsatisfiableCommitQuorum"_sd)
        return UnsatisfiableCommitQuorum;
    if (name == "ClientDisconnect"_sd)
        return ClientDisconnect;
    if (name == "ChangeStreamFatalError"_sd)
        return ChangeStreamFatalError;
    if (name == "TransactionCoordinatorSteppingDown"_sd)
        return TransactionCoordinatorSteppingDown;
    if (name == "TransactionCoordinatorReachedAbortDecision"_sd)
        return TransactionCoordinatorReachedAbortDecision;
    if (name == "WouldChangeOwningShard"_sd)
        return WouldChangeOwningShard;
    if (name == "ForTestingErrorExtraInfoWithExtraInfoInNamespace"_sd)
        return ForTestingErrorExtraInfoWithExtraInfoInNamespace;
    if (name == "IndexBuildAlreadyInProgress"_sd)
        return IndexBuildAlreadyInProgress;
    if (name == "ChangeStreamHistoryLost"_sd)
        return ChangeStreamHistoryLost;
    if (name == "TransactionCoordinatorDeadlineTaskCanceled"_sd)
        return TransactionCoordinatorDeadlineTaskCanceled;
    if (name == "ChecksumMismatch"_sd)
        return ChecksumMismatch;
    if (name == "WaitForMajorityServiceEarlierOpTimeAvailable"_sd)
        return WaitForMajorityServiceEarlierOpTimeAvailable;
    if (name == "TransactionExceededLifetimeLimitSeconds"_sd)
        return TransactionExceededLifetimeLimitSeconds;
    if (name == "NoQueryExecutionPlans"_sd)
        return NoQueryExecutionPlans;
    if (name == "QueryExceededMemoryLimitNoDiskUseAllowed"_sd)
        return QueryExceededMemoryLimitNoDiskUseAllowed;
    if (name == "InvalidSeedList"_sd)
        return InvalidSeedList;
    if (name == "InvalidTopologyType"_sd)
        return InvalidTopologyType;
    if (name == "InvalidHeartBeatFrequency"_sd)
        return InvalidHeartBeatFrequency;
    if (name == "TopologySetNameRequired"_sd)
        return TopologySetNameRequired;
    if (name == "HierarchicalAcquisitionLevelViolation"_sd)
        return HierarchicalAcquisitionLevelViolation;
    if (name == "InvalidServerType"_sd)
        return InvalidServerType;
    if (name == "OCSPCertificateStatusRevoked"_sd)
        return OCSPCertificateStatusRevoked;
    if (name == "RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist"_sd)
        return RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist;
    if (name == "DataCorruptionDetected"_sd)
        return DataCorruptionDetected;
    if (name == "OCSPCertificateStatusUnknown"_sd)
        return OCSPCertificateStatusUnknown;
    if (name == "SplitHorizonChange"_sd)
        return SplitHorizonChange;
    if (name == "ShardInvalidatedForTargeting"_sd)
        return ShardInvalidatedForTargeting;
    if (name == "ReadThroughCacheLookupCanceled"_sd)
        return ReadThroughCacheLookupCanceled;
    if (name == "RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist"_sd)
        return RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist;
    if (name == "CurrentConfigNotCommittedYet"_sd)
        return CurrentConfigNotCommittedYet;
    if (name == "ExhaustCommandFinished"_sd)
        return ExhaustCommandFinished;
    if (name == "PeriodicJobIsStopped"_sd)
        return PeriodicJobIsStopped;
    if (name == "TransactionCoordinatorCanceled"_sd)
        return TransactionCoordinatorCanceled;
    if (name == "OperationIsKilledAndDelisted"_sd)
        return OperationIsKilledAndDelisted;
    if (name == "ResumableRangeDeleterDisabled"_sd)
        return ResumableRangeDeleterDisabled;
    if (name == "ObjectIsBusy"_sd)
        return ObjectIsBusy;
    if (name == "TooStaleToSyncFromSource"_sd)
        return TooStaleToSyncFromSource;
    if (name == "QueryTrialRunCompleted"_sd)
        return QueryTrialRunCompleted;
    if (name == "ConnectionPoolExpired"_sd)
        return ConnectionPoolExpired;
    if (name == "ForTestingOptionalErrorExtraInfo"_sd)
        return ForTestingOptionalErrorExtraInfo;
    if (name == "MovePrimaryInProgress"_sd)
        return MovePrimaryInProgress;
    if (name == "TenantMigrationConflict"_sd)
        return TenantMigrationConflict;
    if (name == "TenantMigrationCommitted"_sd)
        return TenantMigrationCommitted;
    if (name == "APIVersionError"_sd)
        return APIVersionError;
    if (name == "APIStrictError"_sd)
        return APIStrictError;
    if (name == "APIDeprecationError"_sd)
        return APIDeprecationError;
    if (name == "TenantMigrationAborted"_sd)
        return TenantMigrationAborted;
    if (name == "OplogQueryMinTsMissing"_sd)
        return OplogQueryMinTsMissing;
    if (name == "NoSuchTenantMigration"_sd)
        return NoSuchTenantMigration;
    if (name == "TenantMigrationAccessBlockerShuttingDown"_sd)
        return TenantMigrationAccessBlockerShuttingDown;
    if (name == "TenantMigrationInProgress"_sd)
        return TenantMigrationInProgress;
    if (name == "SkipCommandExecution"_sd)
        return SkipCommandExecution;
    if (name == "FailedToRunWithReplyBuilder"_sd)
        return FailedToRunWithReplyBuilder;
    if (name == "CannotDowngrade"_sd)
        return CannotDowngrade;
    if (name == "ServiceExecutorInShutdown"_sd)
        return ServiceExecutorInShutdown;
    if (name == "MechanismUnavailable"_sd)
        return MechanismUnavailable;
    if (name == "TenantMigrationForgotten"_sd)
        return TenantMigrationForgotten;
    if (name == "TimeseriesBucketCleared"_sd)
        return TimeseriesBucketCleared;
    if (name == "AuthenticationAbandoned"_sd)
        return AuthenticationAbandoned;
    if (name == "ReshardCollectionInProgress"_sd)
        return ReshardCollectionInProgress;
    if (name == "NoSuchReshardCollection"_sd)
        return NoSuchReshardCollection;
    if (name == "ReshardCollectionCommitted"_sd)
        return ReshardCollectionCommitted;
    if (name == "ReshardCollectionAborted"_sd)
        return ReshardCollectionAborted;
    if (name == "ReshardingCriticalSectionTimeout"_sd)
        return ReshardingCriticalSectionTimeout;
    if (name == "ShardCannotRefreshDueToLocksHeld"_sd)
        return ShardCannotRefreshDueToLocksHeld;
    if (name == "AuditingNotEnabled"_sd)
        return AuditingNotEnabled;
    if (name == "RuntimeAuditConfigurationNotEnabled"_sd)
        return RuntimeAuditConfigurationNotEnabled;
    if (name == "ChangeStreamInvalidated"_sd)
        return ChangeStreamInvalidated;
    if (name == "APIMismatchError"_sd)
        return APIMismatchError;
    if (name == "ChangeStreamTopologyChange"_sd)
        return ChangeStreamTopologyChange;
    if (name == "KeyPatternShorterThanBound"_sd)
        return KeyPatternShorterThanBound;
    if (name == "ReshardCollectionTruncatedError"_sd)
        return ReshardCollectionTruncatedError;
    if (name == "ChangeStreamStartAfterInvalidate"_sd)
        return ChangeStreamStartAfterInvalidate;
    if (name == "UnsupportedOpQueryCommand"_sd)
        return UnsupportedOpQueryCommand;
    if (name == "NonRetryableTenantMigrationConflict"_sd)
        return NonRetryableTenantMigrationConflict;
    if (name == "LoadBalancerSupportMismatch"_sd)
        return LoadBalancerSupportMismatch;
    if (name == "InterruptedDueToStorageChange"_sd)
        return InterruptedDueToStorageChange;
    if (name == "TxnRetryCounterTooOld"_sd)
        return TxnRetryCounterTooOld;
    if (name == "InvalidBSONType"_sd)
        return InvalidBSONType;
    if (name == "InternalTransactionNotSupported"_sd)
        return InternalTransactionNotSupported;
    if (name == "CannotConvertIndexToUnique"_sd)
        return CannotConvertIndexToUnique;
    if (name == "ShardVersionRefreshCanceled"_sd)
        return ShardVersionRefreshCanceled;
    if (name == "CollectionUUIDMismatch"_sd)
        return CollectionUUIDMismatch;
    if (name == "FutureAlreadyRetrieved"_sd)
        return FutureAlreadyRetrieved;
    if (name == "RetryableTransactionInProgress"_sd)
        return RetryableTransactionInProgress;
    if (name == "TemporarilyUnavailable"_sd)
        return TemporarilyUnavailable;
    if (name == "WouldChangeOwningShardDeletedNoDocument"_sd)
        return WouldChangeOwningShardDeletedNoDocument;
    if (name == "FLECompactionPlaceholder"_sd)
        return FLECompactionPlaceholder;
    if (name == "FLETransactionAbort"_sd)
        return FLETransactionAbort;
    if (name == "CannotDropShardKeyIndex"_sd)
        return CannotDropShardKeyIndex;
    if (name == "UserWritesBlocked"_sd)
        return UserWritesBlocked;
    if (name == "CloseConnectionForShutdownCommand"_sd)
        return CloseConnectionForShutdownCommand;
    if (name == "InternalTransactionsExhaustiveFindHasMore"_sd)
        return InternalTransactionsExhaustiveFindHasMore;
    if (name == "TransactionAPIMustRetryTransaction"_sd)
        return TransactionAPIMustRetryTransaction;
    if (name == "TransactionAPIMustRetryCommit"_sd)
        return TransactionAPIMustRetryCommit;
    if (name == "ChangeStreamNotEnabled"_sd)
        return ChangeStreamNotEnabled;
    if (name == "FLEMaxTagLimitExceeded"_sd)
        return FLEMaxTagLimitExceeded;
    if (name == "NonConformantBSON"_sd)
        return NonConformantBSON;
    if (name == "DatabaseMetadataRefreshCanceled"_sd)
        return DatabaseMetadataRefreshCanceled;
    if (name == "RequestAlreadyFulfilled"_sd)
        return RequestAlreadyFulfilled;
    if (name == "ReshardingCoordinatorServiceConflictingOperationInProgress"_sd)
        return ReshardingCoordinatorServiceConflictingOperationInProgress;
    if (name == "RemoteCommandExecutionError"_sd)
        return RemoteCommandExecutionError;
    if (name == "CollectionIsEmptyLocally"_sd)
        return CollectionIsEmptyLocally;
    if (name == "ConnectionError"_sd)
        return ConnectionError;
    if (name == "ConflictingServerlessOperation"_sd)
        return ConflictingServerlessOperation;
    if (name == "SocketException"_sd)
        return SocketException;
    if (name == "OBSOLETE_RecvStaleConfig"_sd)
        return OBSOLETE_RecvStaleConfig;
    if (name == "CannotGrowDocumentInCappedNamespace"_sd)
        return CannotGrowDocumentInCappedNamespace;
    if (name == "LegacyNotPrimary"_sd)
        return LegacyNotPrimary;
    if (name == "NotWritablePrimary"_sd)
        return NotWritablePrimary;
    if (name == "BSONObjectTooLarge"_sd)
        return BSONObjectTooLarge;
    if (name == "DuplicateKey"_sd)
        return DuplicateKey;
    if (name == "InterruptedAtShutdown"_sd)
        return InterruptedAtShutdown;
    if (name == "Interrupted"_sd)
        return Interrupted;
    if (name == "InterruptedDueToReplStateChange"_sd)
        return InterruptedDueToReplStateChange;
    if (name == "BackgroundOperationInProgressForDatabase"_sd)
        return BackgroundOperationInProgressForDatabase;
    if (name == "BackgroundOperationInProgressForNamespace"_sd)
        return BackgroundOperationInProgressForNamespace;
    if (name == "OBSOLETE_PrepareConfigsFailed"_sd)
        return OBSOLETE_PrepareConfigsFailed;
    if (name == "MergeStageNoMatchingDocument"_sd)
        return MergeStageNoMatchingDocument;
    if (name == "DatabaseDifferCase"_sd)
        return DatabaseDifferCase;
    if (name == "StaleConfig"_sd)
        return StaleConfig;
    if (name == "NotPrimaryNoSecondaryOk"_sd)
        return NotPrimaryNoSecondaryOk;
    if (name == "NotPrimaryOrSecondary"_sd)
        return NotPrimaryOrSecondary;
    if (name == "OutOfDiskSpace"_sd)
        return OutOfDiskSpace;
    if (name == "OBSOLETE_KeyTooLong"_sd)
        return OBSOLETE_KeyTooLong;
    if (name == "ClientMarkedKilled"_sd)
        return ClientMarkedKilled;
    if (name == "NotARetryableWriteCommand"_sd)
        return NotARetryableWriteCommand;
    if (name == "BackupCursorOpenConflictWithCheckpoint"_sd)
        return BackupCursorOpenConflictWithCheckpoint;
    if (name == "ConfigServerUnreachable"_sd)
        return ConfigServerUnreachable;
    return UnknownError;
}

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code) {
    return stream << ErrorCodes::errorString(code);
}

template <>
bool ErrorCodes::isA<ErrorCategory::NetworkError>(Error err) {
    switch (err) {
        case HostUnreachable:
            return true;
        case HostNotFound:
            return true;
        case NetworkTimeout:
            return true;
        case ConnectionPoolExpired:
            return true;
        case ConnectionError:
            return true;
        case SocketException:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::NetworkTimeoutError>(Error err) {
    switch (err) {
        case NetworkTimeout:
            return true;
        case NetworkInterfaceExceededTimeLimit:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::Interruption>(Error err) {
    switch (err) {
        case LockTimeout:
            return true;
        case MaxTimeMSExpired:
            return true;
        case CursorKilled:
            return true;
        case ExceededTimeLimit:
            return true;
        case ClientDisconnect:
            return true;
        case TransactionCoordinatorSteppingDown:
            return true;
        case TransactionCoordinatorReachedAbortDecision:
            return true;
        case TransactionExceededLifetimeLimitSeconds:
            return true;
        case InterruptedDueToStorageChange:
            return true;
        case InterruptedAtShutdown:
            return true;
        case Interrupted:
            return true;
        case InterruptedDueToReplStateChange:
            return true;
        case ClientMarkedKilled:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::NotPrimaryError>(Error err) {
    switch (err) {
        case PrimarySteppedDown:
            return true;
        case NotWritablePrimary:
            return true;
        case InterruptedDueToReplStateChange:
            return true;
        case NotPrimaryNoSecondaryOk:
            return true;
        case NotPrimaryOrSecondary:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::StaleShardVersionError>(Error err) {
    switch (err) {
        case StaleEpoch:
            return true;
        case StaleConfig:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::NeedRetargettingError>(Error err) {
    switch (err) {
        case StaleEpoch:
            return true;
        case StaleConfig:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::WriteConcernError>(Error err) {
    switch (err) {
        case WriteConcernFailed:
            return true;
        case WriteConcernLegacyOK:
            return true;
        case UnknownReplWriteConcern:
            return true;
        case UnsatisfiableWriteConcern:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::ShutdownError>(Error err) {
    switch (err) {
        case ShutdownInProgress:
            return true;
        case ServiceExecutorInShutdown:
            return true;
        case InterruptedAtShutdown:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::CancellationError>(Error err) {
    switch (err) {
        case CallbackCanceled:
            return true;
        case ShutdownInProgress:
            return true;
        case PeriodicJobIsStopped:
            return true;
        case OperationIsKilledAndDelisted:
            return true;
        case ServiceExecutorInShutdown:
            return true;
        case InterruptedDueToStorageChange:
            return true;
        case InterruptedAtShutdown:
            return true;
        case ClientMarkedKilled:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::ConnectionFatalMessageParseError>(Error err) {
    switch (err) {
        case IllegalOpMsgFlag:
            return true;
        case TooManyDocumentSequences:
            return true;
        case ChecksumMismatch:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::ExceededTimeLimitError>(Error err) {
    switch (err) {
        case MaxTimeMSExpired:
            return true;
        case NetworkInterfaceExceededTimeLimit:
            return true;
        case ExceededTimeLimit:
            return true;
        case TransactionExceededLifetimeLimitSeconds:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::SnapshotError>(Error err) {
    switch (err) {
        case SnapshotTooOld:
            return true;
        case SnapshotUnavailable:
            return true;
        case StaleChunkHistory:
            return true;
        case MigrationConflict:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::VoteAbortError>(Error err) {
    switch (err) {
        case ReadConcernMajorityNotEnabled:
            return true;
        case TransactionTooOld:
            return true;
        case NoSuchTransaction:
            return true;
        case OperationNotSupportedInTransaction:
            return true;
        case APIMismatchError:
            return true;
        case TxnRetryCounterTooOld:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::NonResumableChangeStreamError>(Error err) {
    switch (err) {
        case ChangeStreamFatalError:
            return true;
        case ChangeStreamHistoryLost:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::RetriableError>(Error err) {
    switch (err) {
        case HostUnreachable:
            return true;
        case HostNotFound:
            return true;
        case NetworkTimeout:
            return true;
        case ShutdownInProgress:
            return true;
        case ReadConcernMajorityNotAvailableYet:
            return true;
        case PrimarySteppedDown:
            return true;
        case ExceededTimeLimit:
            return true;
        case ConnectionPoolExpired:
            return true;
        case InternalTransactionNotSupported:
            return true;
        case ConnectionError:
            return true;
        case SocketException:
            return true;
        case NotWritablePrimary:
            return true;
        case InterruptedAtShutdown:
            return true;
        case InterruptedDueToReplStateChange:
            return true;
        case NotPrimaryNoSecondaryOk:
            return true;
        case NotPrimaryOrSecondary:
            return true;
        case BackupCursorOpenConflictWithCheckpoint:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::CloseConnectionError>(Error err) {
    switch (err) {
        case SplitHorizonChange:
            return true;
        case LoadBalancerSupportMismatch:
            return true;
        case CloseConnectionForShutdownCommand:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::VersionedAPIError>(Error err) {
    switch (err) {
        case APIVersionError:
            return true;
        case APIStrictError:
            return true;
        case APIDeprecationError:
            return true;
        case APIMismatchError:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::ValidationError>(Error err) {
    switch (err) {
        case Overflow:
            return true;
        case InvalidBSON:
            return true;
        case NonConformantBSON:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::InternalOnly>(Error err) {
    switch (err) {
        case TransactionCoordinatorSteppingDown:
            return true;
        case TransactionCoordinatorReachedAbortDecision:
            return true;
        case TransactionCoordinatorDeadlineTaskCanceled:
            return true;
        case WaitForMajorityServiceEarlierOpTimeAvailable:
            return true;
        case ReadThroughCacheLookupCanceled:
            return true;
        case TransactionCoordinatorCanceled:
            return true;
        case OperationIsKilledAndDelisted:
            return true;
        case TooStaleToSyncFromSource:
            return true;
        case QueryTrialRunCompleted:
            return true;
        case ConnectionPoolExpired:
            return true;
        case TenantMigrationAccessBlockerShuttingDown:
            return true;
        case ServiceExecutorInShutdown:
            return true;
        case TimeseriesBucketCleared:
            return true;
        case AuthenticationAbandoned:
            return true;
        case ShardVersionRefreshCanceled:
            return true;
        case CloseConnectionForShutdownCommand:
            return true;
        case TransactionAPIMustRetryTransaction:
            return true;
        case TransactionAPIMustRetryCommit:
            return true;
        case DatabaseMetadataRefreshCanceled:
            return true;
        case ReshardingCoordinatorServiceConflictingOperationInProgress:
            return true;
        case RemoteCommandExecutionError:
            return true;
        case CollectionIsEmptyLocally:
            return true;
        case ConnectionError:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::TenantMigrationError>(Error err) {
    switch (err) {
        case TenantMigrationConflict:
            return true;
        case TenantMigrationCommitted:
            return true;
        case TenantMigrationAborted:
            return true;
        case NonRetryableTenantMigrationConflict:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::TenantMigrationConflictError>(Error err) {
    switch (err) {
        case TenantMigrationConflict:
            return true;
        case NonRetryableTenantMigrationConflict:
            return true;
        default:
            return false;
    }
}

template <>
bool ErrorCodes::isA<ErrorCategory::CursorInvalidatedError>(Error err) {
    switch (err) {
        case CursorNotFound:
            return true;
        case OperationFailed:
            return true;
        case QueryPlanKilled:
            return true;
        case CursorKilled:
            return true;
        case ObjectIsBusy:
            return true;
        default:
            return false;
    }
}

bool ErrorCodes::canHaveExtraInfo(Error code) {
    switch (code) {
        case ErrorCodes::MultipleErrorsOccurred:
            return true;
        case ErrorCodes::ShutdownInProgress:
            return true;
        case ErrorCodes::DocumentValidationFailure:
            return true;
        case ErrorCodes::StaleEpoch:
            return true;
        case ErrorCodes::CommandOnShardedViewNotSupportedOnMongod:
            return true;
        case ErrorCodes::CannotImplicitlyCreateCollection:
            return true;
        case ErrorCodes::ForTestingErrorExtraInfo:
            return true;
        case ErrorCodes::StaleDbVersion:
            return true;
        case ErrorCodes::JSInterpreterFailureWithStack:
            return true;
        case ErrorCodes::WouldChangeOwningShard:
            return true;
        case ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace:
            return true;
        case ErrorCodes::ShardInvalidatedForTargeting:
            return true;
        case ErrorCodes::ForTestingOptionalErrorExtraInfo:
            return true;
        case ErrorCodes::TenantMigrationConflict:
            return true;
        case ErrorCodes::ShardCannotRefreshDueToLocksHeld:
            return true;
        case ErrorCodes::ChangeStreamInvalidated:
            return true;
        case ErrorCodes::ChangeStreamTopologyChange:
            return true;
        case ErrorCodes::ChangeStreamStartAfterInvalidate:
            return true;
        case ErrorCodes::NonRetryableTenantMigrationConflict:
            return true;
        case ErrorCodes::TxnRetryCounterTooOld:
            return true;
        case ErrorCodes::CannotConvertIndexToUnique:
            return true;
        case ErrorCodes::CollectionUUIDMismatch:
            return true;
        case ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress:
            return true;
        case ErrorCodes::RemoteCommandExecutionError:
            return true;
        case ErrorCodes::DuplicateKey:
            return true;
        case ErrorCodes::StaleConfig:
            return true;
        default:
            return false;
    }
}

bool ErrorCodes::mustHaveExtraInfo(Error code) {
    switch (code) {
        case ErrorCodes::MultipleErrorsOccurred:
            return true;
        case ErrorCodes::DocumentValidationFailure:
            return true;
        case ErrorCodes::CommandOnShardedViewNotSupportedOnMongod:
            return true;
        case ErrorCodes::CannotImplicitlyCreateCollection:
            return true;
        case ErrorCodes::ForTestingErrorExtraInfo:
            return true;
        case ErrorCodes::StaleDbVersion:
            return true;
        case ErrorCodes::JSInterpreterFailureWithStack:
            return true;
        case ErrorCodes::WouldChangeOwningShard:
            return true;
        case ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace:
            return true;
        case ErrorCodes::ShardInvalidatedForTargeting:
            return true;
        case ErrorCodes::TenantMigrationConflict:
            return true;
        case ErrorCodes::ShardCannotRefreshDueToLocksHeld:
            return true;
        case ErrorCodes::ChangeStreamInvalidated:
            return true;
        case ErrorCodes::ChangeStreamTopologyChange:
            return true;
        case ErrorCodes::ChangeStreamStartAfterInvalidate:
            return true;
        case ErrorCodes::NonRetryableTenantMigrationConflict:
            return true;
        case ErrorCodes::TxnRetryCounterTooOld:
            return true;
        case ErrorCodes::CannotConvertIndexToUnique:
            return true;
        case ErrorCodes::CollectionUUIDMismatch:
            return true;
        case ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress:
            return true;
        case ErrorCodes::RemoteCommandExecutionError:
            return true;
        case ErrorCodes::DuplicateKey:
            return true;
        case ErrorCodes::StaleConfig:
            return true;
        default:
            return false;
    }
}

ErrorExtraInfo::Parser* ErrorExtraInfo::parserFor(ErrorCodes::Error code) {
    switch (code) {
        case ErrorCodes::MultipleErrorsOccurred:
            invariant(parsers::MultipleErrorsOccurred);
            return parsers::MultipleErrorsOccurred;
        case ErrorCodes::ShutdownInProgress:
            invariant(parsers::ShutdownInProgress);
            return parsers::ShutdownInProgress;
        case ErrorCodes::DocumentValidationFailure:
            invariant(parsers::DocumentValidationFailure);
            return parsers::DocumentValidationFailure;
        case ErrorCodes::StaleEpoch:
            invariant(parsers::StaleEpoch);
            return parsers::StaleEpoch;
        case ErrorCodes::CommandOnShardedViewNotSupportedOnMongod:
            invariant(parsers::CommandOnShardedViewNotSupportedOnMongod);
            return parsers::CommandOnShardedViewNotSupportedOnMongod;
        case ErrorCodes::CannotImplicitlyCreateCollection:
            invariant(parsers::CannotImplicitlyCreateCollection);
            return parsers::CannotImplicitlyCreateCollection;
        case ErrorCodes::ForTestingErrorExtraInfo:
            invariant(parsers::ForTestingErrorExtraInfo);
            return parsers::ForTestingErrorExtraInfo;
        case ErrorCodes::StaleDbVersion:
            invariant(parsers::StaleDbVersion);
            return parsers::StaleDbVersion;
        case ErrorCodes::JSInterpreterFailureWithStack:
            invariant(parsers::JSInterpreterFailureWithStack);
            return parsers::JSInterpreterFailureWithStack;
        case ErrorCodes::WouldChangeOwningShard:
            invariant(parsers::WouldChangeOwningShard);
            return parsers::WouldChangeOwningShard;
        case ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace:
            invariant(parsers::ForTestingErrorExtraInfoWithExtraInfoInNamespace);
            return parsers::ForTestingErrorExtraInfoWithExtraInfoInNamespace;
        case ErrorCodes::ShardInvalidatedForTargeting:
            invariant(parsers::ShardInvalidatedForTargeting);
            return parsers::ShardInvalidatedForTargeting;
        case ErrorCodes::ForTestingOptionalErrorExtraInfo:
            invariant(parsers::ForTestingOptionalErrorExtraInfo);
            return parsers::ForTestingOptionalErrorExtraInfo;
        case ErrorCodes::TenantMigrationConflict:
            invariant(parsers::TenantMigrationConflict);
            return parsers::TenantMigrationConflict;
        case ErrorCodes::ShardCannotRefreshDueToLocksHeld:
            invariant(parsers::ShardCannotRefreshDueToLocksHeld);
            return parsers::ShardCannotRefreshDueToLocksHeld;
        case ErrorCodes::ChangeStreamInvalidated:
            invariant(parsers::ChangeStreamInvalidated);
            return parsers::ChangeStreamInvalidated;
        case ErrorCodes::ChangeStreamTopologyChange:
            invariant(parsers::ChangeStreamTopologyChange);
            return parsers::ChangeStreamTopologyChange;
        case ErrorCodes::ChangeStreamStartAfterInvalidate:
            invariant(parsers::ChangeStreamStartAfterInvalidate);
            return parsers::ChangeStreamStartAfterInvalidate;
        case ErrorCodes::NonRetryableTenantMigrationConflict:
            invariant(parsers::NonRetryableTenantMigrationConflict);
            return parsers::NonRetryableTenantMigrationConflict;
        case ErrorCodes::TxnRetryCounterTooOld:
            invariant(parsers::TxnRetryCounterTooOld);
            return parsers::TxnRetryCounterTooOld;
        case ErrorCodes::CannotConvertIndexToUnique:
            invariant(parsers::CannotConvertIndexToUnique);
            return parsers::CannotConvertIndexToUnique;
        case ErrorCodes::CollectionUUIDMismatch:
            invariant(parsers::CollectionUUIDMismatch);
            return parsers::CollectionUUIDMismatch;
        case ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress:
            invariant(parsers::ReshardingCoordinatorServiceConflictingOperationInProgress);
            return parsers::ReshardingCoordinatorServiceConflictingOperationInProgress;
        case ErrorCodes::RemoteCommandExecutionError:
            invariant(parsers::RemoteCommandExecutionError);
            return parsers::RemoteCommandExecutionError;
        case ErrorCodes::DuplicateKey:
            invariant(parsers::DuplicateKey);
            return parsers::DuplicateKey;
        case ErrorCodes::StaleConfig:
            invariant(parsers::StaleConfig);
            return parsers::StaleConfig;
        default:
            return nullptr;
    }
}

void ErrorExtraInfo::registerParser(ErrorCodes::Error code, Parser* parser) {
    switch (code) {
        case ErrorCodes::MultipleErrorsOccurred:
            invariant(!parsers::MultipleErrorsOccurred);
            parsers::MultipleErrorsOccurred = parser;
            break;
        case ErrorCodes::ShutdownInProgress:
            invariant(!parsers::ShutdownInProgress);
            parsers::ShutdownInProgress = parser;
            break;
        case ErrorCodes::DocumentValidationFailure:
            invariant(!parsers::DocumentValidationFailure);
            parsers::DocumentValidationFailure = parser;
            break;
        case ErrorCodes::StaleEpoch:
            invariant(!parsers::StaleEpoch);
            parsers::StaleEpoch = parser;
            break;
        case ErrorCodes::CommandOnShardedViewNotSupportedOnMongod:
            invariant(!parsers::CommandOnShardedViewNotSupportedOnMongod);
            parsers::CommandOnShardedViewNotSupportedOnMongod = parser;
            break;
        case ErrorCodes::CannotImplicitlyCreateCollection:
            invariant(!parsers::CannotImplicitlyCreateCollection);
            parsers::CannotImplicitlyCreateCollection = parser;
            break;
        case ErrorCodes::ForTestingErrorExtraInfo:
            invariant(!parsers::ForTestingErrorExtraInfo);
            parsers::ForTestingErrorExtraInfo = parser;
            break;
        case ErrorCodes::StaleDbVersion:
            invariant(!parsers::StaleDbVersion);
            parsers::StaleDbVersion = parser;
            break;
        case ErrorCodes::JSInterpreterFailureWithStack:
            invariant(!parsers::JSInterpreterFailureWithStack);
            parsers::JSInterpreterFailureWithStack = parser;
            break;
        case ErrorCodes::WouldChangeOwningShard:
            invariant(!parsers::WouldChangeOwningShard);
            parsers::WouldChangeOwningShard = parser;
            break;
        case ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace:
            invariant(!parsers::ForTestingErrorExtraInfoWithExtraInfoInNamespace);
            parsers::ForTestingErrorExtraInfoWithExtraInfoInNamespace = parser;
            break;
        case ErrorCodes::ShardInvalidatedForTargeting:
            invariant(!parsers::ShardInvalidatedForTargeting);
            parsers::ShardInvalidatedForTargeting = parser;
            break;
        case ErrorCodes::ForTestingOptionalErrorExtraInfo:
            invariant(!parsers::ForTestingOptionalErrorExtraInfo);
            parsers::ForTestingOptionalErrorExtraInfo = parser;
            break;
        case ErrorCodes::TenantMigrationConflict:
            invariant(!parsers::TenantMigrationConflict);
            parsers::TenantMigrationConflict = parser;
            break;
        case ErrorCodes::ShardCannotRefreshDueToLocksHeld:
            invariant(!parsers::ShardCannotRefreshDueToLocksHeld);
            parsers::ShardCannotRefreshDueToLocksHeld = parser;
            break;
        case ErrorCodes::ChangeStreamInvalidated:
            invariant(!parsers::ChangeStreamInvalidated);
            parsers::ChangeStreamInvalidated = parser;
            break;
        case ErrorCodes::ChangeStreamTopologyChange:
            invariant(!parsers::ChangeStreamTopologyChange);
            parsers::ChangeStreamTopologyChange = parser;
            break;
        case ErrorCodes::ChangeStreamStartAfterInvalidate:
            invariant(!parsers::ChangeStreamStartAfterInvalidate);
            parsers::ChangeStreamStartAfterInvalidate = parser;
            break;
        case ErrorCodes::NonRetryableTenantMigrationConflict:
            invariant(!parsers::NonRetryableTenantMigrationConflict);
            parsers::NonRetryableTenantMigrationConflict = parser;
            break;
        case ErrorCodes::TxnRetryCounterTooOld:
            invariant(!parsers::TxnRetryCounterTooOld);
            parsers::TxnRetryCounterTooOld = parser;
            break;
        case ErrorCodes::CannotConvertIndexToUnique:
            invariant(!parsers::CannotConvertIndexToUnique);
            parsers::CannotConvertIndexToUnique = parser;
            break;
        case ErrorCodes::CollectionUUIDMismatch:
            invariant(!parsers::CollectionUUIDMismatch);
            parsers::CollectionUUIDMismatch = parser;
            break;
        case ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress:
            invariant(!parsers::ReshardingCoordinatorServiceConflictingOperationInProgress);
            parsers::ReshardingCoordinatorServiceConflictingOperationInProgress = parser;
            break;
        case ErrorCodes::RemoteCommandExecutionError:
            invariant(!parsers::RemoteCommandExecutionError);
            parsers::RemoteCommandExecutionError = parser;
            break;
        case ErrorCodes::DuplicateKey:
            invariant(!parsers::DuplicateKey);
            parsers::DuplicateKey = parser;
            break;
        case ErrorCodes::StaleConfig:
            invariant(!parsers::StaleConfig);
            parsers::StaleConfig = parser;
            break;
        default:
            MONGO_UNREACHABLE;
    }
}

void ErrorExtraInfo::invariantHaveAllParsers() {
    invariant(parsers::MultipleErrorsOccurred);
    invariant(parsers::ShutdownInProgress);
    invariant(parsers::DocumentValidationFailure);
    invariant(parsers::StaleEpoch);
    invariant(parsers::CommandOnShardedViewNotSupportedOnMongod);
    invariant(parsers::CannotImplicitlyCreateCollection);
    invariant(parsers::ForTestingErrorExtraInfo);
    invariant(parsers::StaleDbVersion);
    invariant(parsers::JSInterpreterFailureWithStack);
    invariant(parsers::WouldChangeOwningShard);
    invariant(parsers::ForTestingErrorExtraInfoWithExtraInfoInNamespace);
    invariant(parsers::ShardInvalidatedForTargeting);
    invariant(parsers::ForTestingOptionalErrorExtraInfo);
    invariant(parsers::TenantMigrationConflict);
    invariant(parsers::ShardCannotRefreshDueToLocksHeld);
    invariant(parsers::ChangeStreamInvalidated);
    invariant(parsers::ChangeStreamTopologyChange);
    invariant(parsers::ChangeStreamStartAfterInvalidate);
    invariant(parsers::NonRetryableTenantMigrationConflict);
    invariant(parsers::TxnRetryCounterTooOld);
    invariant(parsers::CannotConvertIndexToUnique);
    invariant(parsers::CollectionUUIDMismatch);
    invariant(parsers::ReshardingCoordinatorServiceConflictingOperationInProgress);
    invariant(parsers::RemoteCommandExecutionError);
    invariant(parsers::DuplicateKey);
    invariant(parsers::StaleConfig);
}

void error_details::throwExceptionForStatus(const Status& status) {
    /**
     * This type is used for all exceptions that don't have a more specific type. It is defined
     * locally in this function to prevent anyone from catching it specifically separately from
     * AssertionException.
     */
    class NonspecificAssertionException final : public AssertionException {
    public:
        using AssertionException::AssertionException;

    private:
        void defineOnlyInFinalSubclassToPreventSlicing() final {}
    };

    switch (status.code()) {
        case ErrorCodes::OK:
            throw ExceptionFor<ErrorCodes::OK>(status);
        case ErrorCodes::InternalError:
            throw ExceptionFor<ErrorCodes::InternalError>(status);
        case ErrorCodes::BadValue:
            throw ExceptionFor<ErrorCodes::BadValue>(status);
        case ErrorCodes::OBSOLETE_DuplicateKey:
            throw ExceptionFor<ErrorCodes::OBSOLETE_DuplicateKey>(status);
        case ErrorCodes::NoSuchKey:
            throw ExceptionFor<ErrorCodes::NoSuchKey>(status);
        case ErrorCodes::GraphContainsCycle:
            throw ExceptionFor<ErrorCodes::GraphContainsCycle>(status);
        case ErrorCodes::HostUnreachable:
            throw ExceptionFor<ErrorCodes::HostUnreachable>(status);
        case ErrorCodes::HostNotFound:
            throw ExceptionFor<ErrorCodes::HostNotFound>(status);
        case ErrorCodes::UnknownError:
            throw ExceptionFor<ErrorCodes::UnknownError>(status);
        case ErrorCodes::FailedToParse:
            throw ExceptionFor<ErrorCodes::FailedToParse>(status);
        case ErrorCodes::CannotMutateObject:
            throw ExceptionFor<ErrorCodes::CannotMutateObject>(status);
        case ErrorCodes::UserNotFound:
            throw ExceptionFor<ErrorCodes::UserNotFound>(status);
        case ErrorCodes::UnsupportedFormat:
            throw ExceptionFor<ErrorCodes::UnsupportedFormat>(status);
        case ErrorCodes::Unauthorized:
            throw ExceptionFor<ErrorCodes::Unauthorized>(status);
        case ErrorCodes::TypeMismatch:
            throw ExceptionFor<ErrorCodes::TypeMismatch>(status);
        case ErrorCodes::Overflow:
            throw ExceptionFor<ErrorCodes::Overflow>(status);
        case ErrorCodes::InvalidLength:
            throw ExceptionFor<ErrorCodes::InvalidLength>(status);
        case ErrorCodes::ProtocolError:
            throw ExceptionFor<ErrorCodes::ProtocolError>(status);
        case ErrorCodes::AuthenticationFailed:
            throw ExceptionFor<ErrorCodes::AuthenticationFailed>(status);
        case ErrorCodes::CannotReuseObject:
            throw ExceptionFor<ErrorCodes::CannotReuseObject>(status);
        case ErrorCodes::IllegalOperation:
            throw ExceptionFor<ErrorCodes::IllegalOperation>(status);
        case ErrorCodes::EmptyArrayOperation:
            throw ExceptionFor<ErrorCodes::EmptyArrayOperation>(status);
        case ErrorCodes::InvalidBSON:
            throw ExceptionFor<ErrorCodes::InvalidBSON>(status);
        case ErrorCodes::AlreadyInitialized:
            throw ExceptionFor<ErrorCodes::AlreadyInitialized>(status);
        case ErrorCodes::LockTimeout:
            throw ExceptionFor<ErrorCodes::LockTimeout>(status);
        case ErrorCodes::RemoteValidationError:
            throw ExceptionFor<ErrorCodes::RemoteValidationError>(status);
        case ErrorCodes::NamespaceNotFound:
            throw ExceptionFor<ErrorCodes::NamespaceNotFound>(status);
        case ErrorCodes::IndexNotFound:
            throw ExceptionFor<ErrorCodes::IndexNotFound>(status);
        case ErrorCodes::PathNotViable:
            throw ExceptionFor<ErrorCodes::PathNotViable>(status);
        case ErrorCodes::NonExistentPath:
            throw ExceptionFor<ErrorCodes::NonExistentPath>(status);
        case ErrorCodes::InvalidPath:
            throw ExceptionFor<ErrorCodes::InvalidPath>(status);
        case ErrorCodes::RoleNotFound:
            throw ExceptionFor<ErrorCodes::RoleNotFound>(status);
        case ErrorCodes::RolesNotRelated:
            throw ExceptionFor<ErrorCodes::RolesNotRelated>(status);
        case ErrorCodes::PrivilegeNotFound:
            throw ExceptionFor<ErrorCodes::PrivilegeNotFound>(status);
        case ErrorCodes::CannotBackfillArray:
            throw ExceptionFor<ErrorCodes::CannotBackfillArray>(status);
        case ErrorCodes::UserModificationFailed:
            throw ExceptionFor<ErrorCodes::UserModificationFailed>(status);
        case ErrorCodes::RemoteChangeDetected:
            throw ExceptionFor<ErrorCodes::RemoteChangeDetected>(status);
        case ErrorCodes::FileRenameFailed:
            throw ExceptionFor<ErrorCodes::FileRenameFailed>(status);
        case ErrorCodes::FileNotOpen:
            throw ExceptionFor<ErrorCodes::FileNotOpen>(status);
        case ErrorCodes::FileStreamFailed:
            throw ExceptionFor<ErrorCodes::FileStreamFailed>(status);
        case ErrorCodes::ConflictingUpdateOperators:
            throw ExceptionFor<ErrorCodes::ConflictingUpdateOperators>(status);
        case ErrorCodes::FileAlreadyOpen:
            throw ExceptionFor<ErrorCodes::FileAlreadyOpen>(status);
        case ErrorCodes::LogWriteFailed:
            throw ExceptionFor<ErrorCodes::LogWriteFailed>(status);
        case ErrorCodes::CursorNotFound:
            throw ExceptionFor<ErrorCodes::CursorNotFound>(status);
        case ErrorCodes::UserDataInconsistent:
            throw ExceptionFor<ErrorCodes::UserDataInconsistent>(status);
        case ErrorCodes::LockBusy:
            throw ExceptionFor<ErrorCodes::LockBusy>(status);
        case ErrorCodes::NoMatchingDocument:
            throw ExceptionFor<ErrorCodes::NoMatchingDocument>(status);
        case ErrorCodes::NamespaceExists:
            throw ExceptionFor<ErrorCodes::NamespaceExists>(status);
        case ErrorCodes::InvalidRoleModification:
            throw ExceptionFor<ErrorCodes::InvalidRoleModification>(status);
        case ErrorCodes::MaxTimeMSExpired:
            throw ExceptionFor<ErrorCodes::MaxTimeMSExpired>(status);
        case ErrorCodes::ManualInterventionRequired:
            throw ExceptionFor<ErrorCodes::ManualInterventionRequired>(status);
        case ErrorCodes::DollarPrefixedFieldName:
            throw ExceptionFor<ErrorCodes::DollarPrefixedFieldName>(status);
        case ErrorCodes::InvalidIdField:
            throw ExceptionFor<ErrorCodes::InvalidIdField>(status);
        case ErrorCodes::NotSingleValueField:
            throw ExceptionFor<ErrorCodes::NotSingleValueField>(status);
        case ErrorCodes::InvalidDBRef:
            throw ExceptionFor<ErrorCodes::InvalidDBRef>(status);
        case ErrorCodes::EmptyFieldName:
            throw ExceptionFor<ErrorCodes::EmptyFieldName>(status);
        case ErrorCodes::DottedFieldName:
            throw ExceptionFor<ErrorCodes::DottedFieldName>(status);
        case ErrorCodes::RoleModificationFailed:
            throw ExceptionFor<ErrorCodes::RoleModificationFailed>(status);
        case ErrorCodes::CommandNotFound:
            throw ExceptionFor<ErrorCodes::CommandNotFound>(status);
        case ErrorCodes::OBSOLETE_DatabaseNotFound:
            throw ExceptionFor<ErrorCodes::OBSOLETE_DatabaseNotFound>(status);
        case ErrorCodes::ShardKeyNotFound:
            throw ExceptionFor<ErrorCodes::ShardKeyNotFound>(status);
        case ErrorCodes::OplogOperationUnsupported:
            throw ExceptionFor<ErrorCodes::OplogOperationUnsupported>(status);
        case ErrorCodes::OBSOLETE_StaleShardVersion:
            throw ExceptionFor<ErrorCodes::OBSOLETE_StaleShardVersion>(status);
        case ErrorCodes::WriteConcernFailed:
            throw ExceptionFor<ErrorCodes::WriteConcernFailed>(status);
        case ErrorCodes::MultipleErrorsOccurred:
            throw ExceptionFor<ErrorCodes::MultipleErrorsOccurred>(status);
        case ErrorCodes::ImmutableField:
            throw ExceptionFor<ErrorCodes::ImmutableField>(status);
        case ErrorCodes::CannotCreateIndex:
            throw ExceptionFor<ErrorCodes::CannotCreateIndex>(status);
        case ErrorCodes::IndexAlreadyExists:
            throw ExceptionFor<ErrorCodes::IndexAlreadyExists>(status);
        case ErrorCodes::AuthSchemaIncompatible:
            throw ExceptionFor<ErrorCodes::AuthSchemaIncompatible>(status);
        case ErrorCodes::ShardNotFound:
            throw ExceptionFor<ErrorCodes::ShardNotFound>(status);
        case ErrorCodes::ReplicaSetNotFound:
            throw ExceptionFor<ErrorCodes::ReplicaSetNotFound>(status);
        case ErrorCodes::InvalidOptions:
            throw ExceptionFor<ErrorCodes::InvalidOptions>(status);
        case ErrorCodes::InvalidNamespace:
            throw ExceptionFor<ErrorCodes::InvalidNamespace>(status);
        case ErrorCodes::NodeNotFound:
            throw ExceptionFor<ErrorCodes::NodeNotFound>(status);
        case ErrorCodes::WriteConcernLegacyOK:
            throw ExceptionFor<ErrorCodes::WriteConcernLegacyOK>(status);
        case ErrorCodes::NoReplicationEnabled:
            throw ExceptionFor<ErrorCodes::NoReplicationEnabled>(status);
        case ErrorCodes::OperationIncomplete:
            throw ExceptionFor<ErrorCodes::OperationIncomplete>(status);
        case ErrorCodes::CommandResultSchemaViolation:
            throw ExceptionFor<ErrorCodes::CommandResultSchemaViolation>(status);
        case ErrorCodes::UnknownReplWriteConcern:
            throw ExceptionFor<ErrorCodes::UnknownReplWriteConcern>(status);
        case ErrorCodes::RoleDataInconsistent:
            throw ExceptionFor<ErrorCodes::RoleDataInconsistent>(status);
        case ErrorCodes::NoMatchParseContext:
            throw ExceptionFor<ErrorCodes::NoMatchParseContext>(status);
        case ErrorCodes::NoProgressMade:
            throw ExceptionFor<ErrorCodes::NoProgressMade>(status);
        case ErrorCodes::RemoteResultsUnavailable:
            throw ExceptionFor<ErrorCodes::RemoteResultsUnavailable>(status);
        case ErrorCodes::OBSOLETE_DuplicateKeyValue:
            throw ExceptionFor<ErrorCodes::OBSOLETE_DuplicateKeyValue>(status);
        case ErrorCodes::IndexOptionsConflict:
            throw ExceptionFor<ErrorCodes::IndexOptionsConflict>(status);
        case ErrorCodes::IndexKeySpecsConflict:
            throw ExceptionFor<ErrorCodes::IndexKeySpecsConflict>(status);
        case ErrorCodes::CannotSplit:
            throw ExceptionFor<ErrorCodes::CannotSplit>(status);
        case ErrorCodes::OBSOLETE_SplitFailed:
            throw ExceptionFor<ErrorCodes::OBSOLETE_SplitFailed>(status);
        case ErrorCodes::NetworkTimeout:
            throw ExceptionFor<ErrorCodes::NetworkTimeout>(status);
        case ErrorCodes::CallbackCanceled:
            throw ExceptionFor<ErrorCodes::CallbackCanceled>(status);
        case ErrorCodes::ShutdownInProgress:
            throw ExceptionFor<ErrorCodes::ShutdownInProgress>(status);
        case ErrorCodes::SecondaryAheadOfPrimary:
            throw ExceptionFor<ErrorCodes::SecondaryAheadOfPrimary>(status);
        case ErrorCodes::InvalidReplicaSetConfig:
            throw ExceptionFor<ErrorCodes::InvalidReplicaSetConfig>(status);
        case ErrorCodes::NotYetInitialized:
            throw ExceptionFor<ErrorCodes::NotYetInitialized>(status);
        case ErrorCodes::NotSecondary:
            throw ExceptionFor<ErrorCodes::NotSecondary>(status);
        case ErrorCodes::OperationFailed:
            throw ExceptionFor<ErrorCodes::OperationFailed>(status);
        case ErrorCodes::NoProjectionFound:
            throw ExceptionFor<ErrorCodes::NoProjectionFound>(status);
        case ErrorCodes::DBPathInUse:
            throw ExceptionFor<ErrorCodes::DBPathInUse>(status);
        case ErrorCodes::UnsatisfiableWriteConcern:
            throw ExceptionFor<ErrorCodes::UnsatisfiableWriteConcern>(status);
        case ErrorCodes::OutdatedClient:
            throw ExceptionFor<ErrorCodes::OutdatedClient>(status);
        case ErrorCodes::IncompatibleAuditMetadata:
            throw ExceptionFor<ErrorCodes::IncompatibleAuditMetadata>(status);
        case ErrorCodes::NewReplicaSetConfigurationIncompatible:
            throw ExceptionFor<ErrorCodes::NewReplicaSetConfigurationIncompatible>(status);
        case ErrorCodes::NodeNotElectable:
            throw ExceptionFor<ErrorCodes::NodeNotElectable>(status);
        case ErrorCodes::IncompatibleShardingMetadata:
            throw ExceptionFor<ErrorCodes::IncompatibleShardingMetadata>(status);
        case ErrorCodes::DistributedClockSkewed:
            throw ExceptionFor<ErrorCodes::DistributedClockSkewed>(status);
        case ErrorCodes::LockFailed:
            throw ExceptionFor<ErrorCodes::LockFailed>(status);
        case ErrorCodes::InconsistentReplicaSetNames:
            throw ExceptionFor<ErrorCodes::InconsistentReplicaSetNames>(status);
        case ErrorCodes::ConfigurationInProgress:
            throw ExceptionFor<ErrorCodes::ConfigurationInProgress>(status);
        case ErrorCodes::CannotInitializeNodeWithData:
            throw ExceptionFor<ErrorCodes::CannotInitializeNodeWithData>(status);
        case ErrorCodes::NotExactValueField:
            throw ExceptionFor<ErrorCodes::NotExactValueField>(status);
        case ErrorCodes::WriteConflict:
            throw ExceptionFor<ErrorCodes::WriteConflict>(status);
        case ErrorCodes::InitialSyncFailure:
            throw ExceptionFor<ErrorCodes::InitialSyncFailure>(status);
        case ErrorCodes::InitialSyncOplogSourceMissing:
            throw ExceptionFor<ErrorCodes::InitialSyncOplogSourceMissing>(status);
        case ErrorCodes::CommandNotSupported:
            throw ExceptionFor<ErrorCodes::CommandNotSupported>(status);
        case ErrorCodes::DocTooLargeForCapped:
            throw ExceptionFor<ErrorCodes::DocTooLargeForCapped>(status);
        case ErrorCodes::ConflictingOperationInProgress:
            throw ExceptionFor<ErrorCodes::ConflictingOperationInProgress>(status);
        case ErrorCodes::NamespaceNotSharded:
            throw ExceptionFor<ErrorCodes::NamespaceNotSharded>(status);
        case ErrorCodes::InvalidSyncSource:
            throw ExceptionFor<ErrorCodes::InvalidSyncSource>(status);
        case ErrorCodes::OplogStartMissing:
            throw ExceptionFor<ErrorCodes::OplogStartMissing>(status);
        case ErrorCodes::DocumentValidationFailure:
            throw ExceptionFor<ErrorCodes::DocumentValidationFailure>(status);
        case ErrorCodes::OBSOLETE_ReadAfterOptimeTimeout:
            throw ExceptionFor<ErrorCodes::OBSOLETE_ReadAfterOptimeTimeout>(status);
        case ErrorCodes::NotAReplicaSet:
            throw ExceptionFor<ErrorCodes::NotAReplicaSet>(status);
        case ErrorCodes::IncompatibleElectionProtocol:
            throw ExceptionFor<ErrorCodes::IncompatibleElectionProtocol>(status);
        case ErrorCodes::CommandFailed:
            throw ExceptionFor<ErrorCodes::CommandFailed>(status);
        case ErrorCodes::RPCProtocolNegotiationFailed:
            throw ExceptionFor<ErrorCodes::RPCProtocolNegotiationFailed>(status);
        case ErrorCodes::UnrecoverableRollbackError:
            throw ExceptionFor<ErrorCodes::UnrecoverableRollbackError>(status);
        case ErrorCodes::LockNotFound:
            throw ExceptionFor<ErrorCodes::LockNotFound>(status);
        case ErrorCodes::LockStateChangeFailed:
            throw ExceptionFor<ErrorCodes::LockStateChangeFailed>(status);
        case ErrorCodes::SymbolNotFound:
            throw ExceptionFor<ErrorCodes::SymbolNotFound>(status);
        case ErrorCodes::OBSOLETE_ConfigServersInconsistent:
            throw ExceptionFor<ErrorCodes::OBSOLETE_ConfigServersInconsistent>(status);
        case ErrorCodes::FailedToSatisfyReadPreference:
            throw ExceptionFor<ErrorCodes::FailedToSatisfyReadPreference>(status);
        case ErrorCodes::ReadConcernMajorityNotAvailableYet:
            throw ExceptionFor<ErrorCodes::ReadConcernMajorityNotAvailableYet>(status);
        case ErrorCodes::StaleTerm:
            throw ExceptionFor<ErrorCodes::StaleTerm>(status);
        case ErrorCodes::CappedPositionLost:
            throw ExceptionFor<ErrorCodes::CappedPositionLost>(status);
        case ErrorCodes::IncompatibleShardingConfigVersion:
            throw ExceptionFor<ErrorCodes::IncompatibleShardingConfigVersion>(status);
        case ErrorCodes::RemoteOplogStale:
            throw ExceptionFor<ErrorCodes::RemoteOplogStale>(status);
        case ErrorCodes::JSInterpreterFailure:
            throw ExceptionFor<ErrorCodes::JSInterpreterFailure>(status);
        case ErrorCodes::InvalidSSLConfiguration:
            throw ExceptionFor<ErrorCodes::InvalidSSLConfiguration>(status);
        case ErrorCodes::SSLHandshakeFailed:
            throw ExceptionFor<ErrorCodes::SSLHandshakeFailed>(status);
        case ErrorCodes::JSUncatchableError:
            throw ExceptionFor<ErrorCodes::JSUncatchableError>(status);
        case ErrorCodes::CursorInUse:
            throw ExceptionFor<ErrorCodes::CursorInUse>(status);
        case ErrorCodes::IncompatibleCatalogManager:
            throw ExceptionFor<ErrorCodes::IncompatibleCatalogManager>(status);
        case ErrorCodes::PooledConnectionsDropped:
            throw ExceptionFor<ErrorCodes::PooledConnectionsDropped>(status);
        case ErrorCodes::ExceededMemoryLimit:
            throw ExceptionFor<ErrorCodes::ExceededMemoryLimit>(status);
        case ErrorCodes::ZLibError:
            throw ExceptionFor<ErrorCodes::ZLibError>(status);
        case ErrorCodes::ReadConcernMajorityNotEnabled:
            throw ExceptionFor<ErrorCodes::ReadConcernMajorityNotEnabled>(status);
        case ErrorCodes::NoConfigPrimary:
            throw ExceptionFor<ErrorCodes::NoConfigPrimary>(status);
        case ErrorCodes::StaleEpoch:
            throw ExceptionFor<ErrorCodes::StaleEpoch>(status);
        case ErrorCodes::OperationCannotBeBatched:
            throw ExceptionFor<ErrorCodes::OperationCannotBeBatched>(status);
        case ErrorCodes::OplogOutOfOrder:
            throw ExceptionFor<ErrorCodes::OplogOutOfOrder>(status);
        case ErrorCodes::ChunkTooBig:
            throw ExceptionFor<ErrorCodes::ChunkTooBig>(status);
        case ErrorCodes::InconsistentShardIdentity:
            throw ExceptionFor<ErrorCodes::InconsistentShardIdentity>(status);
        case ErrorCodes::CannotApplyOplogWhilePrimary:
            throw ExceptionFor<ErrorCodes::CannotApplyOplogWhilePrimary>(status);
        case ErrorCodes::OBSOLETE_NeedsDocumentMove:
            throw ExceptionFor<ErrorCodes::OBSOLETE_NeedsDocumentMove>(status);
        case ErrorCodes::CanRepairToDowngrade:
            throw ExceptionFor<ErrorCodes::CanRepairToDowngrade>(status);
        case ErrorCodes::MustUpgrade:
            throw ExceptionFor<ErrorCodes::MustUpgrade>(status);
        case ErrorCodes::DurationOverflow:
            throw ExceptionFor<ErrorCodes::DurationOverflow>(status);
        case ErrorCodes::MaxStalenessOutOfRange:
            throw ExceptionFor<ErrorCodes::MaxStalenessOutOfRange>(status);
        case ErrorCodes::IncompatibleCollationVersion:
            throw ExceptionFor<ErrorCodes::IncompatibleCollationVersion>(status);
        case ErrorCodes::CollectionIsEmpty:
            throw ExceptionFor<ErrorCodes::CollectionIsEmpty>(status);
        case ErrorCodes::ZoneStillInUse:
            throw ExceptionFor<ErrorCodes::ZoneStillInUse>(status);
        case ErrorCodes::InitialSyncActive:
            throw ExceptionFor<ErrorCodes::InitialSyncActive>(status);
        case ErrorCodes::ViewDepthLimitExceeded:
            throw ExceptionFor<ErrorCodes::ViewDepthLimitExceeded>(status);
        case ErrorCodes::CommandNotSupportedOnView:
            throw ExceptionFor<ErrorCodes::CommandNotSupportedOnView>(status);
        case ErrorCodes::OptionNotSupportedOnView:
            throw ExceptionFor<ErrorCodes::OptionNotSupportedOnView>(status);
        case ErrorCodes::InvalidPipelineOperator:
            throw ExceptionFor<ErrorCodes::InvalidPipelineOperator>(status);
        case ErrorCodes::CommandOnShardedViewNotSupportedOnMongod:
            throw ExceptionFor<ErrorCodes::CommandOnShardedViewNotSupportedOnMongod>(status);
        case ErrorCodes::TooManyMatchingDocuments:
            throw ExceptionFor<ErrorCodes::TooManyMatchingDocuments>(status);
        case ErrorCodes::CannotIndexParallelArrays:
            throw ExceptionFor<ErrorCodes::CannotIndexParallelArrays>(status);
        case ErrorCodes::TransportSessionClosed:
            throw ExceptionFor<ErrorCodes::TransportSessionClosed>(status);
        case ErrorCodes::TransportSessionNotFound:
            throw ExceptionFor<ErrorCodes::TransportSessionNotFound>(status);
        case ErrorCodes::TransportSessionUnknown:
            throw ExceptionFor<ErrorCodes::TransportSessionUnknown>(status);
        case ErrorCodes::QueryPlanKilled:
            throw ExceptionFor<ErrorCodes::QueryPlanKilled>(status);
        case ErrorCodes::FileOpenFailed:
            throw ExceptionFor<ErrorCodes::FileOpenFailed>(status);
        case ErrorCodes::ZoneNotFound:
            throw ExceptionFor<ErrorCodes::ZoneNotFound>(status);
        case ErrorCodes::RangeOverlapConflict:
            throw ExceptionFor<ErrorCodes::RangeOverlapConflict>(status);
        case ErrorCodes::WindowsPdhError:
            throw ExceptionFor<ErrorCodes::WindowsPdhError>(status);
        case ErrorCodes::BadPerfCounterPath:
            throw ExceptionFor<ErrorCodes::BadPerfCounterPath>(status);
        case ErrorCodes::AmbiguousIndexKeyPattern:
            throw ExceptionFor<ErrorCodes::AmbiguousIndexKeyPattern>(status);
        case ErrorCodes::InvalidViewDefinition:
            throw ExceptionFor<ErrorCodes::InvalidViewDefinition>(status);
        case ErrorCodes::ClientMetadataMissingField:
            throw ExceptionFor<ErrorCodes::ClientMetadataMissingField>(status);
        case ErrorCodes::ClientMetadataAppNameTooLarge:
            throw ExceptionFor<ErrorCodes::ClientMetadataAppNameTooLarge>(status);
        case ErrorCodes::ClientMetadataDocumentTooLarge:
            throw ExceptionFor<ErrorCodes::ClientMetadataDocumentTooLarge>(status);
        case ErrorCodes::ClientMetadataCannotBeMutated:
            throw ExceptionFor<ErrorCodes::ClientMetadataCannotBeMutated>(status);
        case ErrorCodes::LinearizableReadConcernError:
            throw ExceptionFor<ErrorCodes::LinearizableReadConcernError>(status);
        case ErrorCodes::IncompatibleServerVersion:
            throw ExceptionFor<ErrorCodes::IncompatibleServerVersion>(status);
        case ErrorCodes::PrimarySteppedDown:
            throw ExceptionFor<ErrorCodes::PrimarySteppedDown>(status);
        case ErrorCodes::MasterSlaveConnectionFailure:
            throw ExceptionFor<ErrorCodes::MasterSlaveConnectionFailure>(status);
        case ErrorCodes::OBSOLETE_BalancerLostDistributedLock:
            throw ExceptionFor<ErrorCodes::OBSOLETE_BalancerLostDistributedLock>(status);
        case ErrorCodes::FailPointEnabled:
            throw ExceptionFor<ErrorCodes::FailPointEnabled>(status);
        case ErrorCodes::NoShardingEnabled:
            throw ExceptionFor<ErrorCodes::NoShardingEnabled>(status);
        case ErrorCodes::BalancerInterrupted:
            throw ExceptionFor<ErrorCodes::BalancerInterrupted>(status);
        case ErrorCodes::ViewPipelineMaxSizeExceeded:
            throw ExceptionFor<ErrorCodes::ViewPipelineMaxSizeExceeded>(status);
        case ErrorCodes::InvalidIndexSpecificationOption:
            throw ExceptionFor<ErrorCodes::InvalidIndexSpecificationOption>(status);
        case ErrorCodes::OBSOLETE_ReceivedOpReplyMessage:
            throw ExceptionFor<ErrorCodes::OBSOLETE_ReceivedOpReplyMessage>(status);
        case ErrorCodes::ReplicaSetMonitorRemoved:
            throw ExceptionFor<ErrorCodes::ReplicaSetMonitorRemoved>(status);
        case ErrorCodes::ChunkRangeCleanupPending:
            throw ExceptionFor<ErrorCodes::ChunkRangeCleanupPending>(status);
        case ErrorCodes::CannotBuildIndexKeys:
            throw ExceptionFor<ErrorCodes::CannotBuildIndexKeys>(status);
        case ErrorCodes::NetworkInterfaceExceededTimeLimit:
            throw ExceptionFor<ErrorCodes::NetworkInterfaceExceededTimeLimit>(status);
        case ErrorCodes::ShardingStateNotInitialized:
            throw ExceptionFor<ErrorCodes::ShardingStateNotInitialized>(status);
        case ErrorCodes::TimeProofMismatch:
            throw ExceptionFor<ErrorCodes::TimeProofMismatch>(status);
        case ErrorCodes::ClusterTimeFailsRateLimiter:
            throw ExceptionFor<ErrorCodes::ClusterTimeFailsRateLimiter>(status);
        case ErrorCodes::NoSuchSession:
            throw ExceptionFor<ErrorCodes::NoSuchSession>(status);
        case ErrorCodes::InvalidUUID:
            throw ExceptionFor<ErrorCodes::InvalidUUID>(status);
        case ErrorCodes::TooManyLocks:
            throw ExceptionFor<ErrorCodes::TooManyLocks>(status);
        case ErrorCodes::StaleClusterTime:
            throw ExceptionFor<ErrorCodes::StaleClusterTime>(status);
        case ErrorCodes::CannotVerifyAndSignLogicalTime:
            throw ExceptionFor<ErrorCodes::CannotVerifyAndSignLogicalTime>(status);
        case ErrorCodes::KeyNotFound:
            throw ExceptionFor<ErrorCodes::KeyNotFound>(status);
        case ErrorCodes::IncompatibleRollbackAlgorithm:
            throw ExceptionFor<ErrorCodes::IncompatibleRollbackAlgorithm>(status);
        case ErrorCodes::DuplicateSession:
            throw ExceptionFor<ErrorCodes::DuplicateSession>(status);
        case ErrorCodes::AuthenticationRestrictionUnmet:
            throw ExceptionFor<ErrorCodes::AuthenticationRestrictionUnmet>(status);
        case ErrorCodes::DatabaseDropPending:
            throw ExceptionFor<ErrorCodes::DatabaseDropPending>(status);
        case ErrorCodes::ElectionInProgress:
            throw ExceptionFor<ErrorCodes::ElectionInProgress>(status);
        case ErrorCodes::IncompleteTransactionHistory:
            throw ExceptionFor<ErrorCodes::IncompleteTransactionHistory>(status);
        case ErrorCodes::UpdateOperationFailed:
            throw ExceptionFor<ErrorCodes::UpdateOperationFailed>(status);
        case ErrorCodes::FTDCPathNotSet:
            throw ExceptionFor<ErrorCodes::FTDCPathNotSet>(status);
        case ErrorCodes::FTDCPathAlreadySet:
            throw ExceptionFor<ErrorCodes::FTDCPathAlreadySet>(status);
        case ErrorCodes::IndexModified:
            throw ExceptionFor<ErrorCodes::IndexModified>(status);
        case ErrorCodes::CloseChangeStream:
            throw ExceptionFor<ErrorCodes::CloseChangeStream>(status);
        case ErrorCodes::IllegalOpMsgFlag:
            throw ExceptionFor<ErrorCodes::IllegalOpMsgFlag>(status);
        case ErrorCodes::QueryFeatureNotAllowed:
            throw ExceptionFor<ErrorCodes::QueryFeatureNotAllowed>(status);
        case ErrorCodes::TransactionTooOld:
            throw ExceptionFor<ErrorCodes::TransactionTooOld>(status);
        case ErrorCodes::AtomicityFailure:
            throw ExceptionFor<ErrorCodes::AtomicityFailure>(status);
        case ErrorCodes::CannotImplicitlyCreateCollection:
            throw ExceptionFor<ErrorCodes::CannotImplicitlyCreateCollection>(status);
        case ErrorCodes::SessionTransferIncomplete:
            throw ExceptionFor<ErrorCodes::SessionTransferIncomplete>(status);
        case ErrorCodes::MustDowngrade:
            throw ExceptionFor<ErrorCodes::MustDowngrade>(status);
        case ErrorCodes::DNSHostNotFound:
            throw ExceptionFor<ErrorCodes::DNSHostNotFound>(status);
        case ErrorCodes::DNSProtocolError:
            throw ExceptionFor<ErrorCodes::DNSProtocolError>(status);
        case ErrorCodes::MaxSubPipelineDepthExceeded:
            throw ExceptionFor<ErrorCodes::MaxSubPipelineDepthExceeded>(status);
        case ErrorCodes::TooManyDocumentSequences:
            throw ExceptionFor<ErrorCodes::TooManyDocumentSequences>(status);
        case ErrorCodes::RetryChangeStream:
            throw ExceptionFor<ErrorCodes::RetryChangeStream>(status);
        case ErrorCodes::InternalErrorNotSupported:
            throw ExceptionFor<ErrorCodes::InternalErrorNotSupported>(status);
        case ErrorCodes::ForTestingErrorExtraInfo:
            throw ExceptionFor<ErrorCodes::ForTestingErrorExtraInfo>(status);
        case ErrorCodes::CursorKilled:
            throw ExceptionFor<ErrorCodes::CursorKilled>(status);
        case ErrorCodes::NotImplemented:
            throw ExceptionFor<ErrorCodes::NotImplemented>(status);
        case ErrorCodes::SnapshotTooOld:
            throw ExceptionFor<ErrorCodes::SnapshotTooOld>(status);
        case ErrorCodes::DNSRecordTypeMismatch:
            throw ExceptionFor<ErrorCodes::DNSRecordTypeMismatch>(status);
        case ErrorCodes::ConversionFailure:
            throw ExceptionFor<ErrorCodes::ConversionFailure>(status);
        case ErrorCodes::CannotCreateCollection:
            throw ExceptionFor<ErrorCodes::CannotCreateCollection>(status);
        case ErrorCodes::IncompatibleWithUpgradedServer:
            throw ExceptionFor<ErrorCodes::IncompatibleWithUpgradedServer>(status);
        case ErrorCodes::NOT_YET_AVAILABLE_TransactionAborted:
            throw ExceptionFor<ErrorCodes::NOT_YET_AVAILABLE_TransactionAborted>(status);
        case ErrorCodes::BrokenPromise:
            throw ExceptionFor<ErrorCodes::BrokenPromise>(status);
        case ErrorCodes::SnapshotUnavailable:
            throw ExceptionFor<ErrorCodes::SnapshotUnavailable>(status);
        case ErrorCodes::ProducerConsumerQueueBatchTooLarge:
            throw ExceptionFor<ErrorCodes::ProducerConsumerQueueBatchTooLarge>(status);
        case ErrorCodes::ProducerConsumerQueueEndClosed:
            throw ExceptionFor<ErrorCodes::ProducerConsumerQueueEndClosed>(status);
        case ErrorCodes::StaleDbVersion:
            throw ExceptionFor<ErrorCodes::StaleDbVersion>(status);
        case ErrorCodes::StaleChunkHistory:
            throw ExceptionFor<ErrorCodes::StaleChunkHistory>(status);
        case ErrorCodes::NoSuchTransaction:
            throw ExceptionFor<ErrorCodes::NoSuchTransaction>(status);
        case ErrorCodes::ReentrancyNotAllowed:
            throw ExceptionFor<ErrorCodes::ReentrancyNotAllowed>(status);
        case ErrorCodes::FreeMonHttpInFlight:
            throw ExceptionFor<ErrorCodes::FreeMonHttpInFlight>(status);
        case ErrorCodes::FreeMonHttpTemporaryFailure:
            throw ExceptionFor<ErrorCodes::FreeMonHttpTemporaryFailure>(status);
        case ErrorCodes::FreeMonHttpPermanentFailure:
            throw ExceptionFor<ErrorCodes::FreeMonHttpPermanentFailure>(status);
        case ErrorCodes::TransactionCommitted:
            throw ExceptionFor<ErrorCodes::TransactionCommitted>(status);
        case ErrorCodes::TransactionTooLarge:
            throw ExceptionFor<ErrorCodes::TransactionTooLarge>(status);
        case ErrorCodes::UnknownFeatureCompatibilityVersion:
            throw ExceptionFor<ErrorCodes::UnknownFeatureCompatibilityVersion>(status);
        case ErrorCodes::KeyedExecutorRetry:
            throw ExceptionFor<ErrorCodes::KeyedExecutorRetry>(status);
        case ErrorCodes::InvalidResumeToken:
            throw ExceptionFor<ErrorCodes::InvalidResumeToken>(status);
        case ErrorCodes::TooManyLogicalSessions:
            throw ExceptionFor<ErrorCodes::TooManyLogicalSessions>(status);
        case ErrorCodes::ExceededTimeLimit:
            throw ExceptionFor<ErrorCodes::ExceededTimeLimit>(status);
        case ErrorCodes::OperationNotSupportedInTransaction:
            throw ExceptionFor<ErrorCodes::OperationNotSupportedInTransaction>(status);
        case ErrorCodes::TooManyFilesOpen:
            throw ExceptionFor<ErrorCodes::TooManyFilesOpen>(status);
        case ErrorCodes::OrphanedRangeCleanUpFailed:
            throw ExceptionFor<ErrorCodes::OrphanedRangeCleanUpFailed>(status);
        case ErrorCodes::FailPointSetFailed:
            throw ExceptionFor<ErrorCodes::FailPointSetFailed>(status);
        case ErrorCodes::PreparedTransactionInProgress:
            throw ExceptionFor<ErrorCodes::PreparedTransactionInProgress>(status);
        case ErrorCodes::CannotBackup:
            throw ExceptionFor<ErrorCodes::CannotBackup>(status);
        case ErrorCodes::DataModifiedByRepair:
            throw ExceptionFor<ErrorCodes::DataModifiedByRepair>(status);
        case ErrorCodes::RepairedReplicaSetNode:
            throw ExceptionFor<ErrorCodes::RepairedReplicaSetNode>(status);
        case ErrorCodes::JSInterpreterFailureWithStack:
            throw ExceptionFor<ErrorCodes::JSInterpreterFailureWithStack>(status);
        case ErrorCodes::MigrationConflict:
            throw ExceptionFor<ErrorCodes::MigrationConflict>(status);
        case ErrorCodes::ProducerConsumerQueueProducerQueueDepthExceeded:
            throw ExceptionFor<ErrorCodes::ProducerConsumerQueueProducerQueueDepthExceeded>(status);
        case ErrorCodes::ProducerConsumerQueueConsumed:
            throw ExceptionFor<ErrorCodes::ProducerConsumerQueueConsumed>(status);
        case ErrorCodes::ExchangePassthrough:
            throw ExceptionFor<ErrorCodes::ExchangePassthrough>(status);
        case ErrorCodes::IndexBuildAborted:
            throw ExceptionFor<ErrorCodes::IndexBuildAborted>(status);
        case ErrorCodes::AlarmAlreadyFulfilled:
            throw ExceptionFor<ErrorCodes::AlarmAlreadyFulfilled>(status);
        case ErrorCodes::UnsatisfiableCommitQuorum:
            throw ExceptionFor<ErrorCodes::UnsatisfiableCommitQuorum>(status);
        case ErrorCodes::ClientDisconnect:
            throw ExceptionFor<ErrorCodes::ClientDisconnect>(status);
        case ErrorCodes::ChangeStreamFatalError:
            throw ExceptionFor<ErrorCodes::ChangeStreamFatalError>(status);
        case ErrorCodes::TransactionCoordinatorSteppingDown:
            throw ExceptionFor<ErrorCodes::TransactionCoordinatorSteppingDown>(status);
        case ErrorCodes::TransactionCoordinatorReachedAbortDecision:
            throw ExceptionFor<ErrorCodes::TransactionCoordinatorReachedAbortDecision>(status);
        case ErrorCodes::WouldChangeOwningShard:
            throw ExceptionFor<ErrorCodes::WouldChangeOwningShard>(status);
        case ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace:
            throw ExceptionFor<ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace>(status);
        case ErrorCodes::IndexBuildAlreadyInProgress:
            throw ExceptionFor<ErrorCodes::IndexBuildAlreadyInProgress>(status);
        case ErrorCodes::ChangeStreamHistoryLost:
            throw ExceptionFor<ErrorCodes::ChangeStreamHistoryLost>(status);
        case ErrorCodes::TransactionCoordinatorDeadlineTaskCanceled:
            throw ExceptionFor<ErrorCodes::TransactionCoordinatorDeadlineTaskCanceled>(status);
        case ErrorCodes::ChecksumMismatch:
            throw ExceptionFor<ErrorCodes::ChecksumMismatch>(status);
        case ErrorCodes::WaitForMajorityServiceEarlierOpTimeAvailable:
            throw ExceptionFor<ErrorCodes::WaitForMajorityServiceEarlierOpTimeAvailable>(status);
        case ErrorCodes::TransactionExceededLifetimeLimitSeconds:
            throw ExceptionFor<ErrorCodes::TransactionExceededLifetimeLimitSeconds>(status);
        case ErrorCodes::NoQueryExecutionPlans:
            throw ExceptionFor<ErrorCodes::NoQueryExecutionPlans>(status);
        case ErrorCodes::QueryExceededMemoryLimitNoDiskUseAllowed:
            throw ExceptionFor<ErrorCodes::QueryExceededMemoryLimitNoDiskUseAllowed>(status);
        case ErrorCodes::InvalidSeedList:
            throw ExceptionFor<ErrorCodes::InvalidSeedList>(status);
        case ErrorCodes::InvalidTopologyType:
            throw ExceptionFor<ErrorCodes::InvalidTopologyType>(status);
        case ErrorCodes::InvalidHeartBeatFrequency:
            throw ExceptionFor<ErrorCodes::InvalidHeartBeatFrequency>(status);
        case ErrorCodes::TopologySetNameRequired:
            throw ExceptionFor<ErrorCodes::TopologySetNameRequired>(status);
        case ErrorCodes::HierarchicalAcquisitionLevelViolation:
            throw ExceptionFor<ErrorCodes::HierarchicalAcquisitionLevelViolation>(status);
        case ErrorCodes::InvalidServerType:
            throw ExceptionFor<ErrorCodes::InvalidServerType>(status);
        case ErrorCodes::OCSPCertificateStatusRevoked:
            throw ExceptionFor<ErrorCodes::OCSPCertificateStatusRevoked>(status);
        case ErrorCodes::RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist:
            throw ExceptionFor<ErrorCodes::RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist>(status);
        case ErrorCodes::DataCorruptionDetected:
            throw ExceptionFor<ErrorCodes::DataCorruptionDetected>(status);
        case ErrorCodes::OCSPCertificateStatusUnknown:
            throw ExceptionFor<ErrorCodes::OCSPCertificateStatusUnknown>(status);
        case ErrorCodes::SplitHorizonChange:
            throw ExceptionFor<ErrorCodes::SplitHorizonChange>(status);
        case ErrorCodes::ShardInvalidatedForTargeting:
            throw ExceptionFor<ErrorCodes::ShardInvalidatedForTargeting>(status);
        case ErrorCodes::ReadThroughCacheLookupCanceled:
            throw ExceptionFor<ErrorCodes::ReadThroughCacheLookupCanceled>(status);
        case ErrorCodes::RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist:
            throw ExceptionFor<ErrorCodes::RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist>(status);
        case ErrorCodes::CurrentConfigNotCommittedYet:
            throw ExceptionFor<ErrorCodes::CurrentConfigNotCommittedYet>(status);
        case ErrorCodes::ExhaustCommandFinished:
            throw ExceptionFor<ErrorCodes::ExhaustCommandFinished>(status);
        case ErrorCodes::PeriodicJobIsStopped:
            throw ExceptionFor<ErrorCodes::PeriodicJobIsStopped>(status);
        case ErrorCodes::TransactionCoordinatorCanceled:
            throw ExceptionFor<ErrorCodes::TransactionCoordinatorCanceled>(status);
        case ErrorCodes::OperationIsKilledAndDelisted:
            throw ExceptionFor<ErrorCodes::OperationIsKilledAndDelisted>(status);
        case ErrorCodes::ResumableRangeDeleterDisabled:
            throw ExceptionFor<ErrorCodes::ResumableRangeDeleterDisabled>(status);
        case ErrorCodes::ObjectIsBusy:
            throw ExceptionFor<ErrorCodes::ObjectIsBusy>(status);
        case ErrorCodes::TooStaleToSyncFromSource:
            throw ExceptionFor<ErrorCodes::TooStaleToSyncFromSource>(status);
        case ErrorCodes::QueryTrialRunCompleted:
            throw ExceptionFor<ErrorCodes::QueryTrialRunCompleted>(status);
        case ErrorCodes::ConnectionPoolExpired:
            throw ExceptionFor<ErrorCodes::ConnectionPoolExpired>(status);
        case ErrorCodes::ForTestingOptionalErrorExtraInfo:
            throw ExceptionFor<ErrorCodes::ForTestingOptionalErrorExtraInfo>(status);
        case ErrorCodes::MovePrimaryInProgress:
            throw ExceptionFor<ErrorCodes::MovePrimaryInProgress>(status);
        case ErrorCodes::TenantMigrationConflict:
            throw ExceptionFor<ErrorCodes::TenantMigrationConflict>(status);
        case ErrorCodes::TenantMigrationCommitted:
            throw ExceptionFor<ErrorCodes::TenantMigrationCommitted>(status);
        case ErrorCodes::APIVersionError:
            throw ExceptionFor<ErrorCodes::APIVersionError>(status);
        case ErrorCodes::APIStrictError:
            throw ExceptionFor<ErrorCodes::APIStrictError>(status);
        case ErrorCodes::APIDeprecationError:
            throw ExceptionFor<ErrorCodes::APIDeprecationError>(status);
        case ErrorCodes::TenantMigrationAborted:
            throw ExceptionFor<ErrorCodes::TenantMigrationAborted>(status);
        case ErrorCodes::OplogQueryMinTsMissing:
            throw ExceptionFor<ErrorCodes::OplogQueryMinTsMissing>(status);
        case ErrorCodes::NoSuchTenantMigration:
            throw ExceptionFor<ErrorCodes::NoSuchTenantMigration>(status);
        case ErrorCodes::TenantMigrationAccessBlockerShuttingDown:
            throw ExceptionFor<ErrorCodes::TenantMigrationAccessBlockerShuttingDown>(status);
        case ErrorCodes::TenantMigrationInProgress:
            throw ExceptionFor<ErrorCodes::TenantMigrationInProgress>(status);
        case ErrorCodes::SkipCommandExecution:
            throw ExceptionFor<ErrorCodes::SkipCommandExecution>(status);
        case ErrorCodes::FailedToRunWithReplyBuilder:
            throw ExceptionFor<ErrorCodes::FailedToRunWithReplyBuilder>(status);
        case ErrorCodes::CannotDowngrade:
            throw ExceptionFor<ErrorCodes::CannotDowngrade>(status);
        case ErrorCodes::ServiceExecutorInShutdown:
            throw ExceptionFor<ErrorCodes::ServiceExecutorInShutdown>(status);
        case ErrorCodes::MechanismUnavailable:
            throw ExceptionFor<ErrorCodes::MechanismUnavailable>(status);
        case ErrorCodes::TenantMigrationForgotten:
            throw ExceptionFor<ErrorCodes::TenantMigrationForgotten>(status);
        case ErrorCodes::TimeseriesBucketCleared:
            throw ExceptionFor<ErrorCodes::TimeseriesBucketCleared>(status);
        case ErrorCodes::AuthenticationAbandoned:
            throw ExceptionFor<ErrorCodes::AuthenticationAbandoned>(status);
        case ErrorCodes::ReshardCollectionInProgress:
            throw ExceptionFor<ErrorCodes::ReshardCollectionInProgress>(status);
        case ErrorCodes::NoSuchReshardCollection:
            throw ExceptionFor<ErrorCodes::NoSuchReshardCollection>(status);
        case ErrorCodes::ReshardCollectionCommitted:
            throw ExceptionFor<ErrorCodes::ReshardCollectionCommitted>(status);
        case ErrorCodes::ReshardCollectionAborted:
            throw ExceptionFor<ErrorCodes::ReshardCollectionAborted>(status);
        case ErrorCodes::ReshardingCriticalSectionTimeout:
            throw ExceptionFor<ErrorCodes::ReshardingCriticalSectionTimeout>(status);
        case ErrorCodes::ShardCannotRefreshDueToLocksHeld:
            throw ExceptionFor<ErrorCodes::ShardCannotRefreshDueToLocksHeld>(status);
        case ErrorCodes::AuditingNotEnabled:
            throw ExceptionFor<ErrorCodes::AuditingNotEnabled>(status);
        case ErrorCodes::RuntimeAuditConfigurationNotEnabled:
            throw ExceptionFor<ErrorCodes::RuntimeAuditConfigurationNotEnabled>(status);
        case ErrorCodes::ChangeStreamInvalidated:
            throw ExceptionFor<ErrorCodes::ChangeStreamInvalidated>(status);
        case ErrorCodes::APIMismatchError:
            throw ExceptionFor<ErrorCodes::APIMismatchError>(status);
        case ErrorCodes::ChangeStreamTopologyChange:
            throw ExceptionFor<ErrorCodes::ChangeStreamTopologyChange>(status);
        case ErrorCodes::KeyPatternShorterThanBound:
            throw ExceptionFor<ErrorCodes::KeyPatternShorterThanBound>(status);
        case ErrorCodes::ReshardCollectionTruncatedError:
            throw ExceptionFor<ErrorCodes::ReshardCollectionTruncatedError>(status);
        case ErrorCodes::ChangeStreamStartAfterInvalidate:
            throw ExceptionFor<ErrorCodes::ChangeStreamStartAfterInvalidate>(status);
        case ErrorCodes::UnsupportedOpQueryCommand:
            throw ExceptionFor<ErrorCodes::UnsupportedOpQueryCommand>(status);
        case ErrorCodes::NonRetryableTenantMigrationConflict:
            throw ExceptionFor<ErrorCodes::NonRetryableTenantMigrationConflict>(status);
        case ErrorCodes::LoadBalancerSupportMismatch:
            throw ExceptionFor<ErrorCodes::LoadBalancerSupportMismatch>(status);
        case ErrorCodes::InterruptedDueToStorageChange:
            throw ExceptionFor<ErrorCodes::InterruptedDueToStorageChange>(status);
        case ErrorCodes::TxnRetryCounterTooOld:
            throw ExceptionFor<ErrorCodes::TxnRetryCounterTooOld>(status);
        case ErrorCodes::InvalidBSONType:
            throw ExceptionFor<ErrorCodes::InvalidBSONType>(status);
        case ErrorCodes::InternalTransactionNotSupported:
            throw ExceptionFor<ErrorCodes::InternalTransactionNotSupported>(status);
        case ErrorCodes::CannotConvertIndexToUnique:
            throw ExceptionFor<ErrorCodes::CannotConvertIndexToUnique>(status);
        case ErrorCodes::ShardVersionRefreshCanceled:
            throw ExceptionFor<ErrorCodes::ShardVersionRefreshCanceled>(status);
        case ErrorCodes::CollectionUUIDMismatch:
            throw ExceptionFor<ErrorCodes::CollectionUUIDMismatch>(status);
        case ErrorCodes::FutureAlreadyRetrieved:
            throw ExceptionFor<ErrorCodes::FutureAlreadyRetrieved>(status);
        case ErrorCodes::RetryableTransactionInProgress:
            throw ExceptionFor<ErrorCodes::RetryableTransactionInProgress>(status);
        case ErrorCodes::TemporarilyUnavailable:
            throw ExceptionFor<ErrorCodes::TemporarilyUnavailable>(status);
        case ErrorCodes::WouldChangeOwningShardDeletedNoDocument:
            throw ExceptionFor<ErrorCodes::WouldChangeOwningShardDeletedNoDocument>(status);
        case ErrorCodes::FLECompactionPlaceholder:
            throw ExceptionFor<ErrorCodes::FLECompactionPlaceholder>(status);
        case ErrorCodes::FLETransactionAbort:
            throw ExceptionFor<ErrorCodes::FLETransactionAbort>(status);
        case ErrorCodes::CannotDropShardKeyIndex:
            throw ExceptionFor<ErrorCodes::CannotDropShardKeyIndex>(status);
        case ErrorCodes::UserWritesBlocked:
            throw ExceptionFor<ErrorCodes::UserWritesBlocked>(status);
        case ErrorCodes::CloseConnectionForShutdownCommand:
            throw ExceptionFor<ErrorCodes::CloseConnectionForShutdownCommand>(status);
        case ErrorCodes::InternalTransactionsExhaustiveFindHasMore:
            throw ExceptionFor<ErrorCodes::InternalTransactionsExhaustiveFindHasMore>(status);
        case ErrorCodes::TransactionAPIMustRetryTransaction:
            throw ExceptionFor<ErrorCodes::TransactionAPIMustRetryTransaction>(status);
        case ErrorCodes::TransactionAPIMustRetryCommit:
            throw ExceptionFor<ErrorCodes::TransactionAPIMustRetryCommit>(status);
        case ErrorCodes::ChangeStreamNotEnabled:
            throw ExceptionFor<ErrorCodes::ChangeStreamNotEnabled>(status);
        case ErrorCodes::FLEMaxTagLimitExceeded:
            throw ExceptionFor<ErrorCodes::FLEMaxTagLimitExceeded>(status);
        case ErrorCodes::NonConformantBSON:
            throw ExceptionFor<ErrorCodes::NonConformantBSON>(status);
        case ErrorCodes::DatabaseMetadataRefreshCanceled:
            throw ExceptionFor<ErrorCodes::DatabaseMetadataRefreshCanceled>(status);
        case ErrorCodes::RequestAlreadyFulfilled:
            throw ExceptionFor<ErrorCodes::RequestAlreadyFulfilled>(status);
        case ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress:
            throw ExceptionFor<ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress>(status);
        case ErrorCodes::RemoteCommandExecutionError:
            throw ExceptionFor<ErrorCodes::RemoteCommandExecutionError>(status);
        case ErrorCodes::CollectionIsEmptyLocally:
            throw ExceptionFor<ErrorCodes::CollectionIsEmptyLocally>(status);
        case ErrorCodes::ConnectionError:
            throw ExceptionFor<ErrorCodes::ConnectionError>(status);
        case ErrorCodes::ConflictingServerlessOperation:
            throw ExceptionFor<ErrorCodes::ConflictingServerlessOperation>(status);
        case ErrorCodes::SocketException:
            throw ExceptionFor<ErrorCodes::SocketException>(status);
        case ErrorCodes::OBSOLETE_RecvStaleConfig:
            throw ExceptionFor<ErrorCodes::OBSOLETE_RecvStaleConfig>(status);
        case ErrorCodes::CannotGrowDocumentInCappedNamespace:
            throw ExceptionFor<ErrorCodes::CannotGrowDocumentInCappedNamespace>(status);
        case ErrorCodes::LegacyNotPrimary:
            throw ExceptionFor<ErrorCodes::LegacyNotPrimary>(status);
        case ErrorCodes::NotWritablePrimary:
            throw ExceptionFor<ErrorCodes::NotWritablePrimary>(status);
        case ErrorCodes::BSONObjectTooLarge:
            throw ExceptionFor<ErrorCodes::BSONObjectTooLarge>(status);
        case ErrorCodes::DuplicateKey:
            throw ExceptionFor<ErrorCodes::DuplicateKey>(status);
        case ErrorCodes::InterruptedAtShutdown:
            throw ExceptionFor<ErrorCodes::InterruptedAtShutdown>(status);
        case ErrorCodes::Interrupted:
            throw ExceptionFor<ErrorCodes::Interrupted>(status);
        case ErrorCodes::InterruptedDueToReplStateChange:
            throw ExceptionFor<ErrorCodes::InterruptedDueToReplStateChange>(status);
        case ErrorCodes::BackgroundOperationInProgressForDatabase:
            throw ExceptionFor<ErrorCodes::BackgroundOperationInProgressForDatabase>(status);
        case ErrorCodes::BackgroundOperationInProgressForNamespace:
            throw ExceptionFor<ErrorCodes::BackgroundOperationInProgressForNamespace>(status);
        case ErrorCodes::OBSOLETE_PrepareConfigsFailed:
            throw ExceptionFor<ErrorCodes::OBSOLETE_PrepareConfigsFailed>(status);
        case ErrorCodes::MergeStageNoMatchingDocument:
            throw ExceptionFor<ErrorCodes::MergeStageNoMatchingDocument>(status);
        case ErrorCodes::DatabaseDifferCase:
            throw ExceptionFor<ErrorCodes::DatabaseDifferCase>(status);
        case ErrorCodes::StaleConfig:
            throw ExceptionFor<ErrorCodes::StaleConfig>(status);
        case ErrorCodes::NotPrimaryNoSecondaryOk:
            throw ExceptionFor<ErrorCodes::NotPrimaryNoSecondaryOk>(status);
        case ErrorCodes::NotPrimaryOrSecondary:
            throw ExceptionFor<ErrorCodes::NotPrimaryOrSecondary>(status);
        case ErrorCodes::OutOfDiskSpace:
            throw ExceptionFor<ErrorCodes::OutOfDiskSpace>(status);
        case ErrorCodes::OBSOLETE_KeyTooLong:
            throw ExceptionFor<ErrorCodes::OBSOLETE_KeyTooLong>(status);
        case ErrorCodes::ClientMarkedKilled:
            throw ExceptionFor<ErrorCodes::ClientMarkedKilled>(status);
        case ErrorCodes::NotARetryableWriteCommand:
            throw ExceptionFor<ErrorCodes::NotARetryableWriteCommand>(status);
        case ErrorCodes::BackupCursorOpenConflictWithCheckpoint:
            throw ExceptionFor<ErrorCodes::BackupCursorOpenConflictWithCheckpoint>(status);
        case ErrorCodes::ConfigServerUnreachable:
            throw ExceptionFor<ErrorCodes::ConfigServerUnreachable>(status);
        default:
            throw NonspecificAssertionException(status);
    }
}

}  // namespace mongo
