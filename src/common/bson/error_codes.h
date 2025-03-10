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

#ifndef ERROR_CODES_H
#define ERROR_CODES_H

#include <cstdint>
#include <iosfwd>
#include <string>

#include "common/bson/string_data.h"
#include "common/bson/compiler.h"

namespace mongo {

class Status;
class DBException;

// ErrorExtraInfo subclasses:
    class MultipleErrorsOccurredInfo;
    class ShutdownInProgressQuiesceInfo;
namespace doc_validation_error {
    class DocumentValidationFailureInfo;
}  // namespace doc_validation_error
    class StaleEpochInfo;
    class ResolvedView;
    class CannotImplicitlyCreateCollectionInfo;
    class ErrorExtraInfoExample;
    class StaleDbRoutingVersion;
    class JSExceptionInfo;
    class WouldChangeOwningShardInfo;
namespace nested::twice {
    class NestedErrorExtraInfoExample;
}  // namespace nested::twice
    class ShardInvalidatedForTargetingInfo;
    class OptionalErrorExtraInfoExample;
    class TenantMigrationConflictInfo;
    class ShardCannotRefreshDueToLocksHeldInfo;
    class ChangeStreamInvalidationInfo;
    class ChangeStreamTopologyChangeInfo;
    class ChangeStreamStartAfterInvalidateInfo;
    class NonRetryableTenantMigrationConflictInfo;
    class TxnRetryCounterTooOldInfo;
    class CannotConvertIndexToUniqueInfo;
    class CollectionUUIDMismatchInfo;
    class ReshardingCoordinatorServiceConflictingOperationInProgressInfo;
    class RemoteCommandExecutionErrorInfo;
    class DuplicateKeyErrorInfo;
    class StaleConfigInfo;

enum class ErrorCategory {
    NetworkError,
    NetworkTimeoutError,
    Interruption,
    NotPrimaryError,
    StaleShardVersionError,
    NeedRetargettingError,
    WriteConcernError,
    ShutdownError,
    CancellationError,
    ConnectionFatalMessageParseError,
    ExceededTimeLimitError,
    SnapshotError,
    VoteAbortError,
    NonResumableChangeStreamError,
    RetriableError,
    CloseConnectionError,
    VersionedAPIError,
    ValidationError,
    InternalOnly,
    TenantMigrationError,
    TenantMigrationConflictError,
    CursorInvalidatedError,
};

/**
 * This is a generated class containing a table of error codes and their corresponding error
 * strings. The class is derived from the definitions in src/mongo/base/error_codes.yml file and the
 * src/mongo/base/error_codes.tpl.h template.
 *
 * Do not update this file directly. Update src/mongo/base/error_codes.yml instead.
 */
class ErrorCodes {
public:
    // Explicitly 32-bits wide so that non-symbolic values,
    // like uassert codes, are valid.
    enum Error : std::int32_t {
        OK = 0,
        InternalError = 1,
        BadValue = 2,
        OBSOLETE_DuplicateKey = 3,
        NoSuchKey = 4,
        GraphContainsCycle = 5,
        HostUnreachable = 6,
        HostNotFound = 7,
        UnknownError = 8,
        FailedToParse = 9,
        CannotMutateObject = 10,
        UserNotFound = 11,
        UnsupportedFormat = 12,
        Unauthorized = 13,
        TypeMismatch = 14,
        Overflow = 15,
        InvalidLength = 16,
        ProtocolError = 17,
        AuthenticationFailed = 18,
        CannotReuseObject = 19,
        IllegalOperation = 20,
        EmptyArrayOperation = 21,
        InvalidBSON = 22,
        AlreadyInitialized = 23,
        LockTimeout = 24,
        RemoteValidationError = 25,
        NamespaceNotFound = 26,
        IndexNotFound = 27,
        PathNotViable = 28,
        NonExistentPath = 29,
        InvalidPath = 30,
        RoleNotFound = 31,
        RolesNotRelated = 32,
        PrivilegeNotFound = 33,
        CannotBackfillArray = 34,
        UserModificationFailed = 35,
        RemoteChangeDetected = 36,
        FileRenameFailed = 37,
        FileNotOpen = 38,
        FileStreamFailed = 39,
        ConflictingUpdateOperators = 40,
        FileAlreadyOpen = 41,
        LogWriteFailed = 42,
        CursorNotFound = 43,
        UserDataInconsistent = 45,
        LockBusy = 46,
        NoMatchingDocument = 47,
        NamespaceExists = 48,
        InvalidRoleModification = 49,
        MaxTimeMSExpired = 50,
        ManualInterventionRequired = 51,
        DollarPrefixedFieldName = 52,
        InvalidIdField = 53,
        NotSingleValueField = 54,
        InvalidDBRef = 55,
        EmptyFieldName = 56,
        DottedFieldName = 57,
        RoleModificationFailed = 58,
        CommandNotFound = 59,
        OBSOLETE_DatabaseNotFound = 60,
        ShardKeyNotFound = 61,
        OplogOperationUnsupported = 62,
        OBSOLETE_StaleShardVersion = 63,
        WriteConcernFailed = 64,
        MultipleErrorsOccurred = 65,
        ImmutableField = 66,
        CannotCreateIndex = 67,
        IndexAlreadyExists = 68,
        AuthSchemaIncompatible = 69,
        ShardNotFound = 70,
        ReplicaSetNotFound = 71,
        InvalidOptions = 72,
        InvalidNamespace = 73,
        NodeNotFound = 74,
        WriteConcernLegacyOK = 75,
        NoReplicationEnabled = 76,
        OperationIncomplete = 77,
        CommandResultSchemaViolation = 78,
        UnknownReplWriteConcern = 79,
        RoleDataInconsistent = 80,
        NoMatchParseContext = 81,
        NoProgressMade = 82,
        RemoteResultsUnavailable = 83,
        OBSOLETE_DuplicateKeyValue = 84,
        IndexOptionsConflict = 85,
        IndexKeySpecsConflict = 86,
        CannotSplit = 87,
        OBSOLETE_SplitFailed = 88,
        NetworkTimeout = 89,
        CallbackCanceled = 90,
        ShutdownInProgress = 91,
        SecondaryAheadOfPrimary = 92,
        InvalidReplicaSetConfig = 93,
        NotYetInitialized = 94,
        NotSecondary = 95,
        OperationFailed = 96,
        NoProjectionFound = 97,
        DBPathInUse = 98,
        UnsatisfiableWriteConcern = 100,
        OutdatedClient = 101,
        IncompatibleAuditMetadata = 102,
        NewReplicaSetConfigurationIncompatible = 103,
        NodeNotElectable = 104,
        IncompatibleShardingMetadata = 105,
        DistributedClockSkewed = 106,
        LockFailed = 107,
        InconsistentReplicaSetNames = 108,
        ConfigurationInProgress = 109,
        CannotInitializeNodeWithData = 110,
        NotExactValueField = 111,
        WriteConflict = 112,
        InitialSyncFailure = 113,
        InitialSyncOplogSourceMissing = 114,
        CommandNotSupported = 115,
        DocTooLargeForCapped = 116,
        ConflictingOperationInProgress = 117,
        NamespaceNotSharded = 118,
        InvalidSyncSource = 119,
        OplogStartMissing = 120,
        DocumentValidationFailure = 121,
        OBSOLETE_ReadAfterOptimeTimeout = 122,
        NotAReplicaSet = 123,
        IncompatibleElectionProtocol = 124,
        CommandFailed = 125,
        RPCProtocolNegotiationFailed = 126,
        UnrecoverableRollbackError = 127,
        LockNotFound = 128,
        LockStateChangeFailed = 129,
        SymbolNotFound = 130,
        OBSOLETE_ConfigServersInconsistent = 132,
        FailedToSatisfyReadPreference = 133,
        ReadConcernMajorityNotAvailableYet = 134,
        StaleTerm = 135,
        CappedPositionLost = 136,
        IncompatibleShardingConfigVersion = 137,
        RemoteOplogStale = 138,
        JSInterpreterFailure = 139,
        InvalidSSLConfiguration = 140,
        SSLHandshakeFailed = 141,
        JSUncatchableError = 142,
        CursorInUse = 143,
        IncompatibleCatalogManager = 144,
        PooledConnectionsDropped = 145,
        ExceededMemoryLimit = 146,
        ZLibError = 147,
        ReadConcernMajorityNotEnabled = 148,
        NoConfigPrimary = 149,
        StaleEpoch = 150,
        OperationCannotBeBatched = 151,
        OplogOutOfOrder = 152,
        ChunkTooBig = 153,
        InconsistentShardIdentity = 154,
        CannotApplyOplogWhilePrimary = 155,
        OBSOLETE_NeedsDocumentMove = 156,
        CanRepairToDowngrade = 157,
        MustUpgrade = 158,
        DurationOverflow = 159,
        MaxStalenessOutOfRange = 160,
        IncompatibleCollationVersion = 161,
        CollectionIsEmpty = 162,
        ZoneStillInUse = 163,
        InitialSyncActive = 164,
        ViewDepthLimitExceeded = 165,
        CommandNotSupportedOnView = 166,
        OptionNotSupportedOnView = 167,
        InvalidPipelineOperator = 168,
        CommandOnShardedViewNotSupportedOnMongod = 169,
        TooManyMatchingDocuments = 170,
        CannotIndexParallelArrays = 171,
        TransportSessionClosed = 172,
        TransportSessionNotFound = 173,
        TransportSessionUnknown = 174,
        QueryPlanKilled = 175,
        FileOpenFailed = 176,
        ZoneNotFound = 177,
        RangeOverlapConflict = 178,
        WindowsPdhError = 179,
        BadPerfCounterPath = 180,
        AmbiguousIndexKeyPattern = 181,
        InvalidViewDefinition = 182,
        ClientMetadataMissingField = 183,
        ClientMetadataAppNameTooLarge = 184,
        ClientMetadataDocumentTooLarge = 185,
        ClientMetadataCannotBeMutated = 186,
        LinearizableReadConcernError = 187,
        IncompatibleServerVersion = 188,
        PrimarySteppedDown = 189,
        MasterSlaveConnectionFailure = 190,
        OBSOLETE_BalancerLostDistributedLock = 191,
        FailPointEnabled = 192,
        NoShardingEnabled = 193,
        BalancerInterrupted = 194,
        ViewPipelineMaxSizeExceeded = 195,
        InvalidIndexSpecificationOption = 197,
        OBSOLETE_ReceivedOpReplyMessage = 198,
        ReplicaSetMonitorRemoved = 199,
        ChunkRangeCleanupPending = 200,
        CannotBuildIndexKeys = 201,
        NetworkInterfaceExceededTimeLimit = 202,
        ShardingStateNotInitialized = 203,
        TimeProofMismatch = 204,
        ClusterTimeFailsRateLimiter = 205,
        NoSuchSession = 206,
        InvalidUUID = 207,
        TooManyLocks = 208,
        StaleClusterTime = 209,
        CannotVerifyAndSignLogicalTime = 210,
        KeyNotFound = 211,
        IncompatibleRollbackAlgorithm = 212,
        DuplicateSession = 213,
        AuthenticationRestrictionUnmet = 214,
        DatabaseDropPending = 215,
        ElectionInProgress = 216,
        IncompleteTransactionHistory = 217,
        UpdateOperationFailed = 218,
        FTDCPathNotSet = 219,
        FTDCPathAlreadySet = 220,
        IndexModified = 221,
        CloseChangeStream = 222,
        IllegalOpMsgFlag = 223,
        QueryFeatureNotAllowed = 224,
        TransactionTooOld = 225,
        AtomicityFailure = 226,
        CannotImplicitlyCreateCollection = 227,
        SessionTransferIncomplete = 228,
        MustDowngrade = 229,
        DNSHostNotFound = 230,
        DNSProtocolError = 231,
        MaxSubPipelineDepthExceeded = 232,
        TooManyDocumentSequences = 233,
        RetryChangeStream = 234,
        InternalErrorNotSupported = 235,
        ForTestingErrorExtraInfo = 236,
        CursorKilled = 237,
        NotImplemented = 238,
        SnapshotTooOld = 239,
        DNSRecordTypeMismatch = 240,
        ConversionFailure = 241,
        CannotCreateCollection = 242,
        IncompatibleWithUpgradedServer = 243,
        NOT_YET_AVAILABLE_TransactionAborted = 244,
        BrokenPromise = 245,
        SnapshotUnavailable = 246,
        ProducerConsumerQueueBatchTooLarge = 247,
        ProducerConsumerQueueEndClosed = 248,
        StaleDbVersion = 249,
        StaleChunkHistory = 250,
        NoSuchTransaction = 251,
        ReentrancyNotAllowed = 252,
        FreeMonHttpInFlight = 253,
        FreeMonHttpTemporaryFailure = 254,
        FreeMonHttpPermanentFailure = 255,
        TransactionCommitted = 256,
        TransactionTooLarge = 257,
        UnknownFeatureCompatibilityVersion = 258,
        KeyedExecutorRetry = 259,
        InvalidResumeToken = 260,
        TooManyLogicalSessions = 261,
        ExceededTimeLimit = 262,
        OperationNotSupportedInTransaction = 263,
        TooManyFilesOpen = 264,
        OrphanedRangeCleanUpFailed = 265,
        FailPointSetFailed = 266,
        PreparedTransactionInProgress = 267,
        CannotBackup = 268,
        DataModifiedByRepair = 269,
        RepairedReplicaSetNode = 270,
        JSInterpreterFailureWithStack = 271,
        MigrationConflict = 272,
        ProducerConsumerQueueProducerQueueDepthExceeded = 273,
        ProducerConsumerQueueConsumed = 274,
        ExchangePassthrough = 275,
        IndexBuildAborted = 276,
        AlarmAlreadyFulfilled = 277,
        UnsatisfiableCommitQuorum = 278,
        ClientDisconnect = 279,
        ChangeStreamFatalError = 280,
        TransactionCoordinatorSteppingDown = 281,
        TransactionCoordinatorReachedAbortDecision = 282,
        WouldChangeOwningShard = 283,
        ForTestingErrorExtraInfoWithExtraInfoInNamespace = 284,
        IndexBuildAlreadyInProgress = 285,
        ChangeStreamHistoryLost = 286,
        TransactionCoordinatorDeadlineTaskCanceled = 287,
        ChecksumMismatch = 288,
        WaitForMajorityServiceEarlierOpTimeAvailable = 289,
        TransactionExceededLifetimeLimitSeconds = 290,
        NoQueryExecutionPlans = 291,
        QueryExceededMemoryLimitNoDiskUseAllowed = 292,
        InvalidSeedList = 293,
        InvalidTopologyType = 294,
        InvalidHeartBeatFrequency = 295,
        TopologySetNameRequired = 296,
        HierarchicalAcquisitionLevelViolation = 297,
        InvalidServerType = 298,
        OCSPCertificateStatusRevoked = 299,
        RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist = 300,
        DataCorruptionDetected = 301,
        OCSPCertificateStatusUnknown = 302,
        SplitHorizonChange = 303,
        ShardInvalidatedForTargeting = 304,
        ReadThroughCacheLookupCanceled = 306,
        RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist = 307,
        CurrentConfigNotCommittedYet = 308,
        ExhaustCommandFinished = 309,
        PeriodicJobIsStopped = 310,
        TransactionCoordinatorCanceled = 311,
        OperationIsKilledAndDelisted = 312,
        ResumableRangeDeleterDisabled = 313,
        ObjectIsBusy = 314,
        TooStaleToSyncFromSource = 315,
        QueryTrialRunCompleted = 316,
        ConnectionPoolExpired = 317,
        ForTestingOptionalErrorExtraInfo = 318,
        MovePrimaryInProgress = 319,
        TenantMigrationConflict = 320,
        TenantMigrationCommitted = 321,
        APIVersionError = 322,
        APIStrictError = 323,
        APIDeprecationError = 324,
        TenantMigrationAborted = 325,
        OplogQueryMinTsMissing = 326,
        NoSuchTenantMigration = 327,
        TenantMigrationAccessBlockerShuttingDown = 328,
        TenantMigrationInProgress = 329,
        SkipCommandExecution = 330,
        FailedToRunWithReplyBuilder = 331,
        CannotDowngrade = 332,
        ServiceExecutorInShutdown = 333,
        MechanismUnavailable = 334,
        TenantMigrationForgotten = 335,
        TimeseriesBucketCleared = 336,
        AuthenticationAbandoned = 337,
        ReshardCollectionInProgress = 338,
        NoSuchReshardCollection = 339,
        ReshardCollectionCommitted = 340,
        ReshardCollectionAborted = 341,
        ReshardingCriticalSectionTimeout = 342,
        ShardCannotRefreshDueToLocksHeld = 343,
        AuditingNotEnabled = 344,
        RuntimeAuditConfigurationNotEnabled = 345,
        ChangeStreamInvalidated = 346,
        APIMismatchError = 347,
        ChangeStreamTopologyChange = 348,
        KeyPatternShorterThanBound = 349,
        ReshardCollectionTruncatedError = 350,
        ChangeStreamStartAfterInvalidate = 351,
        UnsupportedOpQueryCommand = 352,
        NonRetryableTenantMigrationConflict = 353,
        LoadBalancerSupportMismatch = 354,
        InterruptedDueToStorageChange = 355,
        TxnRetryCounterTooOld = 356,
        InvalidBSONType = 357,
        InternalTransactionNotSupported = 358,
        CannotConvertIndexToUnique = 359,
        ShardVersionRefreshCanceled = 360,
        CollectionUUIDMismatch = 361,
        FutureAlreadyRetrieved = 362,
        RetryableTransactionInProgress = 363,
        TemporarilyUnavailable = 365,
        WouldChangeOwningShardDeletedNoDocument = 366,
        FLECompactionPlaceholder = 367,
        FLETransactionAbort = 369,
        CannotDropShardKeyIndex = 370,
        UserWritesBlocked = 371,
        CloseConnectionForShutdownCommand = 372,
        InternalTransactionsExhaustiveFindHasMore = 373,
        TransactionAPIMustRetryTransaction = 374,
        TransactionAPIMustRetryCommit = 375,
        ChangeStreamNotEnabled = 376,
        FLEMaxTagLimitExceeded = 377,
        NonConformantBSON = 378,
        DatabaseMetadataRefreshCanceled = 379,
        RequestAlreadyFulfilled = 380,
        ReshardingCoordinatorServiceConflictingOperationInProgress = 381,
        RemoteCommandExecutionError = 382,
        CollectionIsEmptyLocally = 383,
        ConnectionError = 384,
        ConflictingServerlessOperation = 385,
        SocketException = 9001,
        OBSOLETE_RecvStaleConfig = 9996,
        CannotGrowDocumentInCappedNamespace = 10003,
        LegacyNotPrimary = 10058,
        NotWritablePrimary = 10107,
        BSONObjectTooLarge = 10334,
        DuplicateKey = 11000,
        InterruptedAtShutdown = 11600,
        Interrupted = 11601,
        InterruptedDueToReplStateChange = 11602,
        BackgroundOperationInProgressForDatabase = 12586,
        BackgroundOperationInProgressForNamespace = 12587,
        OBSOLETE_PrepareConfigsFailed = 13104,
        MergeStageNoMatchingDocument = 13113,
        DatabaseDifferCase = 13297,
        StaleConfig = 13388,
        NotPrimaryNoSecondaryOk = 13435,
        NotPrimaryOrSecondary = 13436,
        OutOfDiskSpace = 14031,
        OBSOLETE_KeyTooLong = 17280,
        ClientMarkedKilled = 46841,
        NotARetryableWriteCommand = 50768,
        BackupCursorOpenConflictWithCheckpoint = 50915,
        ConfigServerUnreachable = 56846,
        MaxError
    };

    static std::string errorString(Error err);

    /**
     * Parses an Error from its "name".  Returns UnknownError if "name" is unrecognized.
     *
     * NOTE: Also returns UnknownError for the string "UnknownError".
     */
    static Error fromString(StringData name);

    /**
     * Reuses a unique numeric code in a way that supresses the duplicate code detection. This
     * should only be used when testing error cases to ensure that the code under test fails in the
     * right place. It should NOT be used in non-test code to either make a new error site (use
     * ErrorCodes::Error(CODE) for that) or to see if a specific failure case occurred (use named
     * codes for that).
     */
    static Error duplicateCodeForTest(int code) {
        return static_cast<Error>(code);
    }

    /**
     * Generic predicate to test if a given error code is in a category.
     *
     * This version is intended to simplify forwarding by Status and DBException. Non-generic
     * callers should just use the specific isCategoryName() methods instead.
     */
    template <ErrorCategory category>
    static bool isA(Error code);

    template <ErrorCategory category, typename ErrorContainer>
    static bool isA(const ErrorContainer& object);

    static bool isNetworkError(Error code);
    template <typename ErrorContainer>
    static bool isNetworkError(const ErrorContainer& object);

    static bool isNetworkTimeoutError(Error code);
    template <typename ErrorContainer>
    static bool isNetworkTimeoutError(const ErrorContainer& object);

    static bool isInterruption(Error code);
    template <typename ErrorContainer>
    static bool isInterruption(const ErrorContainer& object);

    static bool isNotPrimaryError(Error code);
    template <typename ErrorContainer>
    static bool isNotPrimaryError(const ErrorContainer& object);

    static bool isStaleShardVersionError(Error code);
    template <typename ErrorContainer>
    static bool isStaleShardVersionError(const ErrorContainer& object);

    static bool isNeedRetargettingError(Error code);
    template <typename ErrorContainer>
    static bool isNeedRetargettingError(const ErrorContainer& object);

    static bool isWriteConcernError(Error code);
    template <typename ErrorContainer>
    static bool isWriteConcernError(const ErrorContainer& object);

    static bool isShutdownError(Error code);
    template <typename ErrorContainer>
    static bool isShutdownError(const ErrorContainer& object);

    static bool isCancellationError(Error code);
    template <typename ErrorContainer>
    static bool isCancellationError(const ErrorContainer& object);

    static bool isConnectionFatalMessageParseError(Error code);
    template <typename ErrorContainer>
    static bool isConnectionFatalMessageParseError(const ErrorContainer& object);

    static bool isExceededTimeLimitError(Error code);
    template <typename ErrorContainer>
    static bool isExceededTimeLimitError(const ErrorContainer& object);

    static bool isSnapshotError(Error code);
    template <typename ErrorContainer>
    static bool isSnapshotError(const ErrorContainer& object);

    static bool isVoteAbortError(Error code);
    template <typename ErrorContainer>
    static bool isVoteAbortError(const ErrorContainer& object);

    static bool isNonResumableChangeStreamError(Error code);
    template <typename ErrorContainer>
    static bool isNonResumableChangeStreamError(const ErrorContainer& object);

    static bool isRetriableError(Error code);
    template <typename ErrorContainer>
    static bool isRetriableError(const ErrorContainer& object);

    static bool isCloseConnectionError(Error code);
    template <typename ErrorContainer>
    static bool isCloseConnectionError(const ErrorContainer& object);

    static bool isVersionedAPIError(Error code);
    template <typename ErrorContainer>
    static bool isVersionedAPIError(const ErrorContainer& object);

    static bool isValidationError(Error code);
    template <typename ErrorContainer>
    static bool isValidationError(const ErrorContainer& object);

    static bool isInternalOnly(Error code);
    template <typename ErrorContainer>
    static bool isInternalOnly(const ErrorContainer& object);

    static bool isTenantMigrationError(Error code);
    template <typename ErrorContainer>
    static bool isTenantMigrationError(const ErrorContainer& object);

    static bool isTenantMigrationConflictError(Error code);
    template <typename ErrorContainer>
    static bool isTenantMigrationConflictError(const ErrorContainer& object);

    static bool isCursorInvalidatedError(Error code);
    template <typename ErrorContainer>
    static bool isCursorInvalidatedError(const ErrorContainer& object);

    static bool canHaveExtraInfo(Error code);
    static bool mustHaveExtraInfo(Error code);
};

std::ostream& operator<<(std::ostream& stream, ErrorCodes::Error code);

template <ErrorCategory category, typename ErrorContainer>
inline bool ErrorCodes::isA(const ErrorContainer& object) {
    return isA<category>(object.code());
}

// Category function declarations for "NetworkError"
template <>
bool ErrorCodes::isA<ErrorCategory::NetworkError>(Error code);

inline bool ErrorCodes::isNetworkError(Error code) {
    return isA<ErrorCategory::NetworkError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isNetworkError(const ErrorContainer& object) {
    return isA<ErrorCategory::NetworkError>(object.code());
}

// Category function declarations for "NetworkTimeoutError"
template <>
bool ErrorCodes::isA<ErrorCategory::NetworkTimeoutError>(Error code);

inline bool ErrorCodes::isNetworkTimeoutError(Error code) {
    return isA<ErrorCategory::NetworkTimeoutError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isNetworkTimeoutError(const ErrorContainer& object) {
    return isA<ErrorCategory::NetworkTimeoutError>(object.code());
}

// Category function declarations for "Interruption"
template <>
bool ErrorCodes::isA<ErrorCategory::Interruption>(Error code);

inline bool ErrorCodes::isInterruption(Error code) {
    return isA<ErrorCategory::Interruption>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isInterruption(const ErrorContainer& object) {
    return isA<ErrorCategory::Interruption>(object.code());
}

// Category function declarations for "NotPrimaryError"
template <>
bool ErrorCodes::isA<ErrorCategory::NotPrimaryError>(Error code);

inline bool ErrorCodes::isNotPrimaryError(Error code) {
    return isA<ErrorCategory::NotPrimaryError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isNotPrimaryError(const ErrorContainer& object) {
    return isA<ErrorCategory::NotPrimaryError>(object.code());
}

// Category function declarations for "StaleShardVersionError"
template <>
bool ErrorCodes::isA<ErrorCategory::StaleShardVersionError>(Error code);

inline bool ErrorCodes::isStaleShardVersionError(Error code) {
    return isA<ErrorCategory::StaleShardVersionError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isStaleShardVersionError(const ErrorContainer& object) {
    return isA<ErrorCategory::StaleShardVersionError>(object.code());
}

// Category function declarations for "NeedRetargettingError"
template <>
bool ErrorCodes::isA<ErrorCategory::NeedRetargettingError>(Error code);

inline bool ErrorCodes::isNeedRetargettingError(Error code) {
    return isA<ErrorCategory::NeedRetargettingError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isNeedRetargettingError(const ErrorContainer& object) {
    return isA<ErrorCategory::NeedRetargettingError>(object.code());
}

// Category function declarations for "WriteConcernError"
template <>
bool ErrorCodes::isA<ErrorCategory::WriteConcernError>(Error code);

inline bool ErrorCodes::isWriteConcernError(Error code) {
    return isA<ErrorCategory::WriteConcernError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isWriteConcernError(const ErrorContainer& object) {
    return isA<ErrorCategory::WriteConcernError>(object.code());
}

// Category function declarations for "ShutdownError"
template <>
bool ErrorCodes::isA<ErrorCategory::ShutdownError>(Error code);

inline bool ErrorCodes::isShutdownError(Error code) {
    return isA<ErrorCategory::ShutdownError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isShutdownError(const ErrorContainer& object) {
    return isA<ErrorCategory::ShutdownError>(object.code());
}

// Category function declarations for "CancellationError"
template <>
bool ErrorCodes::isA<ErrorCategory::CancellationError>(Error code);

inline bool ErrorCodes::isCancellationError(Error code) {
    return isA<ErrorCategory::CancellationError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isCancellationError(const ErrorContainer& object) {
    return isA<ErrorCategory::CancellationError>(object.code());
}

// Category function declarations for "ConnectionFatalMessageParseError"
template <>
bool ErrorCodes::isA<ErrorCategory::ConnectionFatalMessageParseError>(Error code);

inline bool ErrorCodes::isConnectionFatalMessageParseError(Error code) {
    return isA<ErrorCategory::ConnectionFatalMessageParseError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isConnectionFatalMessageParseError(const ErrorContainer& object) {
    return isA<ErrorCategory::ConnectionFatalMessageParseError>(object.code());
}

// Category function declarations for "ExceededTimeLimitError"
template <>
bool ErrorCodes::isA<ErrorCategory::ExceededTimeLimitError>(Error code);

inline bool ErrorCodes::isExceededTimeLimitError(Error code) {
    return isA<ErrorCategory::ExceededTimeLimitError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isExceededTimeLimitError(const ErrorContainer& object) {
    return isA<ErrorCategory::ExceededTimeLimitError>(object.code());
}

// Category function declarations for "SnapshotError"
template <>
bool ErrorCodes::isA<ErrorCategory::SnapshotError>(Error code);

inline bool ErrorCodes::isSnapshotError(Error code) {
    return isA<ErrorCategory::SnapshotError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isSnapshotError(const ErrorContainer& object) {
    return isA<ErrorCategory::SnapshotError>(object.code());
}

// Category function declarations for "VoteAbortError"
template <>
bool ErrorCodes::isA<ErrorCategory::VoteAbortError>(Error code);

inline bool ErrorCodes::isVoteAbortError(Error code) {
    return isA<ErrorCategory::VoteAbortError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isVoteAbortError(const ErrorContainer& object) {
    return isA<ErrorCategory::VoteAbortError>(object.code());
}

// Category function declarations for "NonResumableChangeStreamError"
template <>
bool ErrorCodes::isA<ErrorCategory::NonResumableChangeStreamError>(Error code);

inline bool ErrorCodes::isNonResumableChangeStreamError(Error code) {
    return isA<ErrorCategory::NonResumableChangeStreamError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isNonResumableChangeStreamError(const ErrorContainer& object) {
    return isA<ErrorCategory::NonResumableChangeStreamError>(object.code());
}

// Category function declarations for "RetriableError"
template <>
bool ErrorCodes::isA<ErrorCategory::RetriableError>(Error code);

inline bool ErrorCodes::isRetriableError(Error code) {
    return isA<ErrorCategory::RetriableError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isRetriableError(const ErrorContainer& object) {
    return isA<ErrorCategory::RetriableError>(object.code());
}

// Category function declarations for "CloseConnectionError"
template <>
bool ErrorCodes::isA<ErrorCategory::CloseConnectionError>(Error code);

inline bool ErrorCodes::isCloseConnectionError(Error code) {
    return isA<ErrorCategory::CloseConnectionError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isCloseConnectionError(const ErrorContainer& object) {
    return isA<ErrorCategory::CloseConnectionError>(object.code());
}

// Category function declarations for "VersionedAPIError"
template <>
bool ErrorCodes::isA<ErrorCategory::VersionedAPIError>(Error code);

inline bool ErrorCodes::isVersionedAPIError(Error code) {
    return isA<ErrorCategory::VersionedAPIError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isVersionedAPIError(const ErrorContainer& object) {
    return isA<ErrorCategory::VersionedAPIError>(object.code());
}

// Category function declarations for "ValidationError"
template <>
bool ErrorCodes::isA<ErrorCategory::ValidationError>(Error code);

inline bool ErrorCodes::isValidationError(Error code) {
    return isA<ErrorCategory::ValidationError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isValidationError(const ErrorContainer& object) {
    return isA<ErrorCategory::ValidationError>(object.code());
}

// Category function declarations for "InternalOnly"
template <>
bool ErrorCodes::isA<ErrorCategory::InternalOnly>(Error code);

inline bool ErrorCodes::isInternalOnly(Error code) {
    return isA<ErrorCategory::InternalOnly>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isInternalOnly(const ErrorContainer& object) {
    return isA<ErrorCategory::InternalOnly>(object.code());
}

// Category function declarations for "TenantMigrationError"
template <>
bool ErrorCodes::isA<ErrorCategory::TenantMigrationError>(Error code);

inline bool ErrorCodes::isTenantMigrationError(Error code) {
    return isA<ErrorCategory::TenantMigrationError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isTenantMigrationError(const ErrorContainer& object) {
    return isA<ErrorCategory::TenantMigrationError>(object.code());
}

// Category function declarations for "TenantMigrationConflictError"
template <>
bool ErrorCodes::isA<ErrorCategory::TenantMigrationConflictError>(Error code);

inline bool ErrorCodes::isTenantMigrationConflictError(Error code) {
    return isA<ErrorCategory::TenantMigrationConflictError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isTenantMigrationConflictError(const ErrorContainer& object) {
    return isA<ErrorCategory::TenantMigrationConflictError>(object.code());
}

// Category function declarations for "CursorInvalidatedError"
template <>
bool ErrorCodes::isA<ErrorCategory::CursorInvalidatedError>(Error code);

inline bool ErrorCodes::isCursorInvalidatedError(Error code) {
    return isA<ErrorCategory::CursorInvalidatedError>(code);
}

template <typename ErrorContainer>
inline bool ErrorCodes::isCursorInvalidatedError(const ErrorContainer& object) {
    return isA<ErrorCategory::CursorInvalidatedError>(object.code());
}

/**
 * This namespace contains implementation details for our error handling code and should not be used
 * directly in general code.
 */
namespace error_details {

template <int32_t code>
constexpr bool isNamedCode = false;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OK> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InternalError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BadValue> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_DuplicateKey> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoSuchKey> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::GraphContainsCycle> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::HostUnreachable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::HostNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnknownError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FailedToParse> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotMutateObject> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UserNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnsupportedFormat> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::Unauthorized> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TypeMismatch> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::Overflow> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidLength> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ProtocolError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AuthenticationFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotReuseObject> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IllegalOperation> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::EmptyArrayOperation> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidBSON> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AlreadyInitialized> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LockTimeout> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RemoteValidationError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NamespaceNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IndexNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::PathNotViable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NonExistentPath> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidPath> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RoleNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RolesNotRelated> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::PrivilegeNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotBackfillArray> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UserModificationFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RemoteChangeDetected> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FileRenameFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FileNotOpen> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FileStreamFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConflictingUpdateOperators> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FileAlreadyOpen> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LogWriteFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CursorNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UserDataInconsistent> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LockBusy> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoMatchingDocument> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NamespaceExists> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidRoleModification> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MaxTimeMSExpired> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ManualInterventionRequired> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DollarPrefixedFieldName> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidIdField> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotSingleValueField> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidDBRef> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::EmptyFieldName> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DottedFieldName> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RoleModificationFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CommandNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_DatabaseNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ShardKeyNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OplogOperationUnsupported> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_StaleShardVersion> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::WriteConcernFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MultipleErrorsOccurred> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ImmutableField> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotCreateIndex> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IndexAlreadyExists> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AuthSchemaIncompatible> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ShardNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReplicaSetNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidOptions> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidNamespace> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NodeNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::WriteConcernLegacyOK> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoReplicationEnabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OperationIncomplete> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CommandResultSchemaViolation> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnknownReplWriteConcern> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RoleDataInconsistent> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoMatchParseContext> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoProgressMade> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RemoteResultsUnavailable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_DuplicateKeyValue> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IndexOptionsConflict> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IndexKeySpecsConflict> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotSplit> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_SplitFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NetworkTimeout> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CallbackCanceled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ShutdownInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SecondaryAheadOfPrimary> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidReplicaSetConfig> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotYetInitialized> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotSecondary> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OperationFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoProjectionFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DBPathInUse> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnsatisfiableWriteConcern> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OutdatedClient> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleAuditMetadata> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NewReplicaSetConfigurationIncompatible> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NodeNotElectable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleShardingMetadata> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DistributedClockSkewed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LockFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InconsistentReplicaSetNames> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConfigurationInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotInitializeNodeWithData> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotExactValueField> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::WriteConflict> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InitialSyncFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InitialSyncOplogSourceMissing> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CommandNotSupported> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DocTooLargeForCapped> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConflictingOperationInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NamespaceNotSharded> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidSyncSource> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OplogStartMissing> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DocumentValidationFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_ReadAfterOptimeTimeout> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotAReplicaSet> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleElectionProtocol> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CommandFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RPCProtocolNegotiationFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnrecoverableRollbackError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LockNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LockStateChangeFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SymbolNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_ConfigServersInconsistent> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FailedToSatisfyReadPreference> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReadConcernMajorityNotAvailableYet> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::StaleTerm> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CappedPositionLost> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleShardingConfigVersion> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RemoteOplogStale> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::JSInterpreterFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidSSLConfiguration> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SSLHandshakeFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::JSUncatchableError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CursorInUse> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleCatalogManager> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::PooledConnectionsDropped> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ExceededMemoryLimit> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ZLibError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReadConcernMajorityNotEnabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoConfigPrimary> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::StaleEpoch> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OperationCannotBeBatched> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OplogOutOfOrder> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChunkTooBig> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InconsistentShardIdentity> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotApplyOplogWhilePrimary> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_NeedsDocumentMove> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CanRepairToDowngrade> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MustUpgrade> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DurationOverflow> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MaxStalenessOutOfRange> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleCollationVersion> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CollectionIsEmpty> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ZoneStillInUse> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InitialSyncActive> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ViewDepthLimitExceeded> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CommandNotSupportedOnView> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OptionNotSupportedOnView> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidPipelineOperator> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CommandOnShardedViewNotSupportedOnMongod> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TooManyMatchingDocuments> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotIndexParallelArrays> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransportSessionClosed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransportSessionNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransportSessionUnknown> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::QueryPlanKilled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FileOpenFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ZoneNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RangeOverlapConflict> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::WindowsPdhError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BadPerfCounterPath> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AmbiguousIndexKeyPattern> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidViewDefinition> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ClientMetadataMissingField> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ClientMetadataAppNameTooLarge> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ClientMetadataDocumentTooLarge> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ClientMetadataCannotBeMutated> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LinearizableReadConcernError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleServerVersion> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::PrimarySteppedDown> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MasterSlaveConnectionFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_BalancerLostDistributedLock> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FailPointEnabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoShardingEnabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BalancerInterrupted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ViewPipelineMaxSizeExceeded> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidIndexSpecificationOption> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_ReceivedOpReplyMessage> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReplicaSetMonitorRemoved> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChunkRangeCleanupPending> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotBuildIndexKeys> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NetworkInterfaceExceededTimeLimit> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ShardingStateNotInitialized> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TimeProofMismatch> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ClusterTimeFailsRateLimiter> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoSuchSession> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidUUID> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TooManyLocks> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::StaleClusterTime> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotVerifyAndSignLogicalTime> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::KeyNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleRollbackAlgorithm> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DuplicateSession> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AuthenticationRestrictionUnmet> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DatabaseDropPending> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ElectionInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompleteTransactionHistory> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UpdateOperationFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FTDCPathNotSet> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FTDCPathAlreadySet> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IndexModified> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CloseChangeStream> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IllegalOpMsgFlag> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::QueryFeatureNotAllowed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionTooOld> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AtomicityFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotImplicitlyCreateCollection> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SessionTransferIncomplete> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MustDowngrade> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DNSHostNotFound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DNSProtocolError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MaxSubPipelineDepthExceeded> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TooManyDocumentSequences> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RetryChangeStream> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InternalErrorNotSupported> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ForTestingErrorExtraInfo> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CursorKilled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotImplemented> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SnapshotTooOld> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DNSRecordTypeMismatch> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConversionFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotCreateCollection> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IncompatibleWithUpgradedServer> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NOT_YET_AVAILABLE_TransactionAborted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BrokenPromise> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SnapshotUnavailable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ProducerConsumerQueueBatchTooLarge> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ProducerConsumerQueueEndClosed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::StaleDbVersion> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::StaleChunkHistory> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoSuchTransaction> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReentrancyNotAllowed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FreeMonHttpInFlight> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FreeMonHttpTemporaryFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FreeMonHttpPermanentFailure> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionCommitted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionTooLarge> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnknownFeatureCompatibilityVersion> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::KeyedExecutorRetry> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidResumeToken> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TooManyLogicalSessions> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ExceededTimeLimit> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OperationNotSupportedInTransaction> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TooManyFilesOpen> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OrphanedRangeCleanUpFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FailPointSetFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::PreparedTransactionInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotBackup> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DataModifiedByRepair> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RepairedReplicaSetNode> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::JSInterpreterFailureWithStack> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MigrationConflict> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ProducerConsumerQueueProducerQueueDepthExceeded> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ProducerConsumerQueueConsumed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ExchangePassthrough> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IndexBuildAborted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AlarmAlreadyFulfilled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnsatisfiableCommitQuorum> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ClientDisconnect> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChangeStreamFatalError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionCoordinatorSteppingDown> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionCoordinatorReachedAbortDecision> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::WouldChangeOwningShard> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::IndexBuildAlreadyInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChangeStreamHistoryLost> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionCoordinatorDeadlineTaskCanceled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChecksumMismatch> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::WaitForMajorityServiceEarlierOpTimeAvailable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionExceededLifetimeLimitSeconds> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoQueryExecutionPlans> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::QueryExceededMemoryLimitNoDiskUseAllowed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidSeedList> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidTopologyType> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidHeartBeatFrequency> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TopologySetNameRequired> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::HierarchicalAcquisitionLevelViolation> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidServerType> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OCSPCertificateStatusRevoked> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RangeDeletionAbandonedBecauseCollectionWithUUIDDoesNotExist> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DataCorruptionDetected> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OCSPCertificateStatusUnknown> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SplitHorizonChange> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ShardInvalidatedForTargeting> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReadThroughCacheLookupCanceled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RangeDeletionAbandonedBecauseTaskDocumentDoesNotExist> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CurrentConfigNotCommittedYet> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ExhaustCommandFinished> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::PeriodicJobIsStopped> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionCoordinatorCanceled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OperationIsKilledAndDelisted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ResumableRangeDeleterDisabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ObjectIsBusy> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TooStaleToSyncFromSource> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::QueryTrialRunCompleted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConnectionPoolExpired> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ForTestingOptionalErrorExtraInfo> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MovePrimaryInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TenantMigrationConflict> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TenantMigrationCommitted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::APIVersionError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::APIStrictError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::APIDeprecationError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TenantMigrationAborted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OplogQueryMinTsMissing> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoSuchTenantMigration> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TenantMigrationAccessBlockerShuttingDown> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TenantMigrationInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SkipCommandExecution> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FailedToRunWithReplyBuilder> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotDowngrade> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ServiceExecutorInShutdown> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MechanismUnavailable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TenantMigrationForgotten> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TimeseriesBucketCleared> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AuthenticationAbandoned> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReshardCollectionInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NoSuchReshardCollection> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReshardCollectionCommitted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReshardCollectionAborted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReshardingCriticalSectionTimeout> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ShardCannotRefreshDueToLocksHeld> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::AuditingNotEnabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RuntimeAuditConfigurationNotEnabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChangeStreamInvalidated> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::APIMismatchError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChangeStreamTopologyChange> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::KeyPatternShorterThanBound> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReshardCollectionTruncatedError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChangeStreamStartAfterInvalidate> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UnsupportedOpQueryCommand> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NonRetryableTenantMigrationConflict> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LoadBalancerSupportMismatch> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InterruptedDueToStorageChange> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TxnRetryCounterTooOld> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InvalidBSONType> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InternalTransactionNotSupported> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotConvertIndexToUnique> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ShardVersionRefreshCanceled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CollectionUUIDMismatch> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FutureAlreadyRetrieved> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RetryableTransactionInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TemporarilyUnavailable> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::WouldChangeOwningShardDeletedNoDocument> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FLECompactionPlaceholder> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FLETransactionAbort> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotDropShardKeyIndex> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::UserWritesBlocked> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CloseConnectionForShutdownCommand> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InternalTransactionsExhaustiveFindHasMore> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionAPIMustRetryTransaction> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::TransactionAPIMustRetryCommit> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ChangeStreamNotEnabled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::FLEMaxTagLimitExceeded> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NonConformantBSON> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DatabaseMetadataRefreshCanceled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RequestAlreadyFulfilled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::RemoteCommandExecutionError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CollectionIsEmptyLocally> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConnectionError> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConflictingServerlessOperation> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::SocketException> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_RecvStaleConfig> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::CannotGrowDocumentInCappedNamespace> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::LegacyNotPrimary> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotWritablePrimary> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BSONObjectTooLarge> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DuplicateKey> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InterruptedAtShutdown> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::Interrupted> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::InterruptedDueToReplStateChange> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BackgroundOperationInProgressForDatabase> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BackgroundOperationInProgressForNamespace> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_PrepareConfigsFailed> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::MergeStageNoMatchingDocument> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::DatabaseDifferCase> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::StaleConfig> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotPrimaryNoSecondaryOk> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotPrimaryOrSecondary> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OutOfDiskSpace> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::OBSOLETE_KeyTooLong> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ClientMarkedKilled> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::NotARetryableWriteCommand> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::BackupCursorOpenConflictWithCheckpoint> = true;
template <>
constexpr inline bool isNamedCode<ErrorCodes::ConfigServerUnreachable> = true;

MONGO_COMPILER_NORETURN void throwExceptionForStatus(const Status& status);

//
// ErrorCategoriesFor
//

template <ErrorCategory... categories>
struct CategoryList;

template <ErrorCodes::Error code>
struct ErrorCategoriesForImpl {
    using type = CategoryList<>;
};

template <>
struct ErrorCategoriesForImpl<ErrorCodes::HostUnreachable> {
    using type = CategoryList<
        ErrorCategory::NetworkError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::HostNotFound> {
    using type = CategoryList<
        ErrorCategory::NetworkError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::Overflow> {
    using type = CategoryList<
        ErrorCategory::ValidationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::InvalidBSON> {
    using type = CategoryList<
        ErrorCategory::ValidationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::LockTimeout> {
    using type = CategoryList<
        ErrorCategory::Interruption
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::CursorNotFound> {
    using type = CategoryList<
        ErrorCategory::CursorInvalidatedError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::MaxTimeMSExpired> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::ExceededTimeLimitError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::WriteConcernFailed> {
    using type = CategoryList<
        ErrorCategory::WriteConcernError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::WriteConcernLegacyOK> {
    using type = CategoryList<
        ErrorCategory::WriteConcernError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::UnknownReplWriteConcern> {
    using type = CategoryList<
        ErrorCategory::WriteConcernError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NetworkTimeout> {
    using type = CategoryList<
        ErrorCategory::NetworkError, 
        ErrorCategory::NetworkTimeoutError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::CallbackCanceled> {
    using type = CategoryList<
        ErrorCategory::CancellationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ShutdownInProgress> {
    using type = CategoryList<
        ErrorCategory::ShutdownError, 
        ErrorCategory::CancellationError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::OperationFailed> {
    using type = CategoryList<
        ErrorCategory::CursorInvalidatedError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::UnsatisfiableWriteConcern> {
    using type = CategoryList<
        ErrorCategory::WriteConcernError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ReadConcernMajorityNotAvailableYet> {
    using type = CategoryList<
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ReadConcernMajorityNotEnabled> {
    using type = CategoryList<
        ErrorCategory::VoteAbortError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::StaleEpoch> {
    using type = CategoryList<
        ErrorCategory::StaleShardVersionError, 
        ErrorCategory::NeedRetargettingError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::QueryPlanKilled> {
    using type = CategoryList<
        ErrorCategory::CursorInvalidatedError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::PrimarySteppedDown> {
    using type = CategoryList<
        ErrorCategory::NotPrimaryError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NetworkInterfaceExceededTimeLimit> {
    using type = CategoryList<
        ErrorCategory::NetworkTimeoutError, 
        ErrorCategory::ExceededTimeLimitError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::IllegalOpMsgFlag> {
    using type = CategoryList<
        ErrorCategory::ConnectionFatalMessageParseError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionTooOld> {
    using type = CategoryList<
        ErrorCategory::VoteAbortError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TooManyDocumentSequences> {
    using type = CategoryList<
        ErrorCategory::ConnectionFatalMessageParseError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::CursorKilled> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::CursorInvalidatedError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::SnapshotTooOld> {
    using type = CategoryList<
        ErrorCategory::SnapshotError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::SnapshotUnavailable> {
    using type = CategoryList<
        ErrorCategory::SnapshotError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::StaleChunkHistory> {
    using type = CategoryList<
        ErrorCategory::SnapshotError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NoSuchTransaction> {
    using type = CategoryList<
        ErrorCategory::VoteAbortError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ExceededTimeLimit> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::ExceededTimeLimitError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::OperationNotSupportedInTransaction> {
    using type = CategoryList<
        ErrorCategory::VoteAbortError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::MigrationConflict> {
    using type = CategoryList<
        ErrorCategory::SnapshotError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ClientDisconnect> {
    using type = CategoryList<
        ErrorCategory::Interruption
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ChangeStreamFatalError> {
    using type = CategoryList<
        ErrorCategory::NonResumableChangeStreamError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionCoordinatorSteppingDown> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionCoordinatorReachedAbortDecision> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ChangeStreamHistoryLost> {
    using type = CategoryList<
        ErrorCategory::NonResumableChangeStreamError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionCoordinatorDeadlineTaskCanceled> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ChecksumMismatch> {
    using type = CategoryList<
        ErrorCategory::ConnectionFatalMessageParseError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::WaitForMajorityServiceEarlierOpTimeAvailable> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionExceededLifetimeLimitSeconds> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::ExceededTimeLimitError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::SplitHorizonChange> {
    using type = CategoryList<
        ErrorCategory::CloseConnectionError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ReadThroughCacheLookupCanceled> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::PeriodicJobIsStopped> {
    using type = CategoryList<
        ErrorCategory::CancellationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionCoordinatorCanceled> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::OperationIsKilledAndDelisted> {
    using type = CategoryList<
        ErrorCategory::CancellationError, 
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ObjectIsBusy> {
    using type = CategoryList<
        ErrorCategory::CursorInvalidatedError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TooStaleToSyncFromSource> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::QueryTrialRunCompleted> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ConnectionPoolExpired> {
    using type = CategoryList<
        ErrorCategory::NetworkError, 
        ErrorCategory::RetriableError, 
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TenantMigrationConflict> {
    using type = CategoryList<
        ErrorCategory::TenantMigrationError, 
        ErrorCategory::TenantMigrationConflictError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TenantMigrationCommitted> {
    using type = CategoryList<
        ErrorCategory::TenantMigrationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::APIVersionError> {
    using type = CategoryList<
        ErrorCategory::VersionedAPIError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::APIStrictError> {
    using type = CategoryList<
        ErrorCategory::VersionedAPIError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::APIDeprecationError> {
    using type = CategoryList<
        ErrorCategory::VersionedAPIError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TenantMigrationAborted> {
    using type = CategoryList<
        ErrorCategory::TenantMigrationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TenantMigrationAccessBlockerShuttingDown> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ServiceExecutorInShutdown> {
    using type = CategoryList<
        ErrorCategory::ShutdownError, 
        ErrorCategory::CancellationError, 
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TimeseriesBucketCleared> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::AuthenticationAbandoned> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::APIMismatchError> {
    using type = CategoryList<
        ErrorCategory::VoteAbortError, 
        ErrorCategory::VersionedAPIError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NonRetryableTenantMigrationConflict> {
    using type = CategoryList<
        ErrorCategory::TenantMigrationError, 
        ErrorCategory::TenantMigrationConflictError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::LoadBalancerSupportMismatch> {
    using type = CategoryList<
        ErrorCategory::CloseConnectionError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::InterruptedDueToStorageChange> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::CancellationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TxnRetryCounterTooOld> {
    using type = CategoryList<
        ErrorCategory::VoteAbortError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::InternalTransactionNotSupported> {
    using type = CategoryList<
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ShardVersionRefreshCanceled> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::CloseConnectionForShutdownCommand> {
    using type = CategoryList<
        ErrorCategory::CloseConnectionError, 
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionAPIMustRetryTransaction> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::TransactionAPIMustRetryCommit> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NonConformantBSON> {
    using type = CategoryList<
        ErrorCategory::ValidationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::DatabaseMetadataRefreshCanceled> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::RemoteCommandExecutionError> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::CollectionIsEmptyLocally> {
    using type = CategoryList<
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ConnectionError> {
    using type = CategoryList<
        ErrorCategory::NetworkError, 
        ErrorCategory::RetriableError, 
        ErrorCategory::InternalOnly
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::SocketException> {
    using type = CategoryList<
        ErrorCategory::NetworkError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NotWritablePrimary> {
    using type = CategoryList<
        ErrorCategory::NotPrimaryError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::InterruptedAtShutdown> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::ShutdownError, 
        ErrorCategory::CancellationError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::Interrupted> {
    using type = CategoryList<
        ErrorCategory::Interruption
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::InterruptedDueToReplStateChange> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::NotPrimaryError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::StaleConfig> {
    using type = CategoryList<
        ErrorCategory::StaleShardVersionError, 
        ErrorCategory::NeedRetargettingError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NotPrimaryNoSecondaryOk> {
    using type = CategoryList<
        ErrorCategory::NotPrimaryError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::NotPrimaryOrSecondary> {
    using type = CategoryList<
        ErrorCategory::NotPrimaryError, 
        ErrorCategory::RetriableError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::ClientMarkedKilled> {
    using type = CategoryList<
        ErrorCategory::Interruption, 
        ErrorCategory::CancellationError
        >;
};
template <>
struct ErrorCategoriesForImpl<ErrorCodes::BackupCursorOpenConflictWithCheckpoint> {
    using type = CategoryList<
        ErrorCategory::RetriableError
        >;
};

template <ErrorCodes::Error code>
using ErrorCategoriesFor = typename ErrorCategoriesForImpl<code>::type;

//
// ErrorExtraInfoFor
//

template <ErrorCodes::Error code>
struct ErrorExtraInfoForImpl {};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::MultipleErrorsOccurred> {
    using type = MultipleErrorsOccurredInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ShutdownInProgress> {
    using type = ShutdownInProgressQuiesceInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::DocumentValidationFailure> {
    using type = doc_validation_error::DocumentValidationFailureInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::StaleEpoch> {
    using type = StaleEpochInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::CommandOnShardedViewNotSupportedOnMongod> {
    using type = ResolvedView;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::CannotImplicitlyCreateCollection> {
    using type = CannotImplicitlyCreateCollectionInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ForTestingErrorExtraInfo> {
    using type = ErrorExtraInfoExample;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::StaleDbVersion> {
    using type = StaleDbRoutingVersion;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::JSInterpreterFailureWithStack> {
    using type = JSExceptionInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::WouldChangeOwningShard> {
    using type = WouldChangeOwningShardInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ForTestingErrorExtraInfoWithExtraInfoInNamespace> {
    using type = nested::twice::NestedErrorExtraInfoExample;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ShardInvalidatedForTargeting> {
    using type = ShardInvalidatedForTargetingInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ForTestingOptionalErrorExtraInfo> {
    using type = OptionalErrorExtraInfoExample;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::TenantMigrationConflict> {
    using type = TenantMigrationConflictInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ShardCannotRefreshDueToLocksHeld> {
    using type = ShardCannotRefreshDueToLocksHeldInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ChangeStreamInvalidated> {
    using type = ChangeStreamInvalidationInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ChangeStreamTopologyChange> {
    using type = ChangeStreamTopologyChangeInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ChangeStreamStartAfterInvalidate> {
    using type = ChangeStreamStartAfterInvalidateInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::NonRetryableTenantMigrationConflict> {
    using type = NonRetryableTenantMigrationConflictInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::TxnRetryCounterTooOld> {
    using type = TxnRetryCounterTooOldInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::CannotConvertIndexToUnique> {
    using type = CannotConvertIndexToUniqueInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::CollectionUUIDMismatch> {
    using type = CollectionUUIDMismatchInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::ReshardingCoordinatorServiceConflictingOperationInProgress> {
    using type = ReshardingCoordinatorServiceConflictingOperationInProgressInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::RemoteCommandExecutionError> {
    using type = RemoteCommandExecutionErrorInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::DuplicateKey> {
    using type = DuplicateKeyErrorInfo;
};

template <>
struct ErrorExtraInfoForImpl<ErrorCodes::StaleConfig> {
    using type = StaleConfigInfo;
};


template <ErrorCodes::Error code>
using ErrorExtraInfoFor = typename ErrorExtraInfoForImpl<code>::type;

}  // namespace error_details

}  // namespace mongo

#endif