## AltaworxPondGetBillingGroups Lambda Flow Documentation

### Overview
Synchronizes billing group data from the Pond API into the database. Supports two modes:
- Initialization mode: seeds page work per inventory and enqueues SQS messages.
- Processing mode: fetches one page from Pond and stages rows into the DB, then signals downstream processing.

### Key Components
- `AwsFunctionBase`: logging, config, DB connections, bulk copy, cleanup
- `PondRepository`: DB CRUD for Pond sync, staging, and progress tracking
- `PondApiService`: list API calls, query param construction, request building
- `ServiceProviderRepository`: service provider enumeration and metadata
- `EnvironmentRepository`: environment variable access
- `SqsService`: SQS message publishing
- `RetryPolicyHelper`: SQL transient retry policy
- `HttpClientSingleton`, `HttpRequestFactory`: HTTP client and request construction

## Environment Variables
- `POND_GET_BILLING_GROUPS_QUEUE_URL` (key: `PondHelper.CommonString.POND_GET_BILLING_GROUPS_QUEUE_URL_VARIABLE_KEY`)
- `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL` (key: `PondHelper.CommonString.POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL_VARIABLE_KEY`)
- `POND_GET_BILLING_GROUP_ENDPOINT` (key: `PondHelper.CommonString.POND_GET_BILLING_GROUP_ENDPOINT_VARIABLE_KEY`)
- `PAGE_SIZE` (key: `PondHelper.CommonString.PAGE_SIZE`, default: `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)

## SQS Message Attributes
- `SERVICE_PROVIDER_ID` (required in processing mode)
- `INVENTORY_ID` (required in processing mode)
- `PAGE_NUMBER` (required in processing mode)
- `IS_SUCCESSFUL` (used for downstream progress signaling)

## Database Tables
- `DatabaseTableNames.PondBillingGroupStaging`
- `DatabaseTableNames.POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS`

## Data Models and Structures
- `SqsValues`
  - `ServiceProviderId: long`
  - `InventoryId: long`
  - `PageNumber: int`
  - `IsSuccessful: bool`
- Staging DataTable schema (`PondBillingGroupStaging`)
  - `Id`, `Parent_Group_Id`, `Ocs_Group_Id`, `Inventory_Id`, `Name`, `Number_Of_Subgroups`, `Number_Of_Sims`, `CreatedDate`, `ServiceProviderId`
- Page-to-process DataTable schema (`POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS`)
  - `PageNumber`, `InventoryId`, `ServiceProviderId`

## High-Level Flow
- Initialization: seed page markers per service provider and inventory; enqueue one SQS per page
- Fetch: pull one page of billing groups using `offset = pageNumber * pageSize`
- Stage: bulk insert to `PondBillingGroupStaging`
- Advance: emit progress messages to `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL`

## Function and Method Reference

### FunctionHandler
- **Signature**: `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
- **Purpose**: Main entry point. Processes SQS messages to orchestrate billing group synchronization.
- **Inputs**:
  - `sqsEvent`: SQS batch event
  - `context`: Lambda context
- **Behavior**:
  - Initializes `AmopLambdaContext` via `BaseAmopFunctionHandler()`.
  - Reads environment variables via `TryGetAllEnvironmentVariables()`.
  - Validates SQS trigger; iterates each record.
  - For each record, parses attributes via `GetMessageValues()`.
  - Branches:
    - If `ServiceProviderId` missing or ≤ 0: calls `InitializeSyncBillingGroupProcess()`.
    - Else: calls `ProcessSyncPageByServiceProviderId()`.
  - Handles exceptions and calls `CleanUp()`.
- **Outputs**: None. Side-effecting orchestration.
- **Side Effects**: Logs, DB connections, SQS calls, HTTP calls via downstream methods.
- **Environment**: Requires all env vars listed above.

### InitializeSyncBillingGroupProcess
- **Signature**: `InitializeSyncBillingGroupProcess(AmopLambdaContext context, ServiceProviderRepository serviceProviderRepository)`
- **Purpose**: Seeds the sync process and fans out per-page SQS messages.
- **Inputs**:
  - `context`: Operational context with logging and config
  - `serviceProviderRepository`: Repository to enumerate Pond service providers
- **Behavior**:
  - `pondRepository.TruncateStagingTables()` to reset staging.
  - Fetches all Pond Service Provider IDs via `GetAllServiceProviderIds(IntegrationType.Pond)`.
  - For each `serviceProviderId`:
    - Gets authentication via `pondRepository.GetPondAuthentication(serviceProviderId)`.
    - Fetches all inventory IDs via `pondRepository.GetAllInventoryIds(serviceProviderId)`.
    - For each `inventoryId`:
      - Builds endpoint via `string.Format(PondGetBillingGroupEndpoint, inventoryId)`.
      - Calls `pondApiService.TryGetTotalPageCount<PondBillingGroupItem>` to determine `totalPages`.
      - Calls `LoadPagesToProcessTable(context, serviceProviderId, inventoryId, totalPages)` to seed DB page markers.
      - For `page` in `[0, totalPages)` calls `InitGetBillingGroupPages(context, serviceProviderId, inventoryId, page)` to enqueue SQS messages to `POND_GET_BILLING_GROUPS_QUEUE_URL`.
- **Outputs**: None. Seeds DB and SQS.
- **Side Effects**: Truncates staging tables, bulk inserts into page-to-process table, publishes SQS messages.
- **Errors**: Auth failures, API failures, SQL errors.

### ProcessSyncPageByServiceProviderId
- **Signature**: `ProcessSyncPageByServiceProviderId(AmopLambdaContext context, SqsValues sqsValues)`
- **Purpose**: Processes a single page of billing groups for a given inventory.
- **Inputs**:
  - `context`: Operational context
  - `sqsValues`: `ServiceProviderId`, `InventoryId`, `PageNumber`
- **Behavior**:
  - Retrieves auth via `pondRepository.GetPondAuthentication(sqsValues.ServiceProviderId)`.
  - Instantiates `PondApiService`.
  - Calls `SyncBillingGroup(context, sqsValues, sqlTransientRetryPolicy, pondApiService)`.
- **Outputs**: None. Side-effecting load + progress signal.
- **Side Effects**: HTTP to Pond API, SQL bulk copy, emits downstream SQS progress message.

### SyncBillingGroup
- **Signature**: `SyncBillingGroup(AmopLambdaContext context, SqsValues sqsValues, IAsyncPolicy sqlTransientRetryPolicy, PondApiService pondApiService)`
- **Purpose**: Orchestrates single-page fetch, staging load, and progress signaling using retry policy.
- **Inputs**:
  - `context`
  - `sqsValues`
  - `sqlTransientRetryPolicy`: SQL retry policy from `RetryPolicyHelper`
  - `pondApiService`
- **Behavior**:
  - Calls `GetSinglePageListFromPondAPIAsync<PondBillingGroupItem, PondBillingGroupListResponse>` using computed offset.
  - Calls `LoadBillingGroupToStagingTable(context, serviceProviderId, inventoryId, items)` to bulk stage.
  - Calls `CheckSyncBillingGroupStepProgress(context, sqsValues, isSuccessful)` to emit progress/completion message.
- **Outputs**: None.
- **Side Effects**: DB writes and SQS publish.

### GetSinglePageListFromPondAPIAsync
- **Signature**: `GetSinglePageListFromPondAPIAsync<TItem, TListResponse>(PondApiService pondApiService, string formattedEndpoint, int pageNumber, int pageSize)`
- **Purpose**: Fetches a single page of items from Pond API.
- **Inputs**:
  - `pondApiService`
  - `formattedEndpoint`: e.g., `string.Format(POND_GET_BILLING_GROUP_ENDPOINT, inventoryId)`
  - `pageNumber`, `pageSize`
- **Behavior**:
  - Computes `offset = pageNumber * pageSize`.
  - Calls `pondApiService.GetPondListAsync<TListResponse>(HttpClientSingleton.Instance, formattedEndpoint, offset, pageSize)`.
  - Extracts list via `response => response.Elements`.
- **Outputs**: `IEnumerable<TItem>` for the page.
- **Side Effects**: HTTP request to Pond API.

### LoadBillingGroupToStagingTable
- **Signature**: `LoadBillingGroupToStagingTable(AmopLambdaContext context, long serviceProviderId, long inventoryId, IEnumerable<PondBillingGroupItem> items)`
- **Purpose**: Bulk loads billing group rows into staging table.
- **Inputs**:
  - `context`, `serviceProviderId`, `inventoryId`, `items`
- **Behavior**:
  - Shapes a `DataTable` with columns: `Id`, `Parent_Group_Id`, `Ocs_Group_Id`, `Inventory_Id`, `Name`, `Number_Of_Subgroups`, `Number_Of_Sims`, `CreatedDate`, `ServiceProviderId`.
  - Executes `SqlBulkCopy` into `DatabaseTableNames.PondBillingGroupStaging`.
- **Outputs**: Count/none (side-effect only).
- **Side Effects**: SQL bulk insert.
- **Errors**: Schema mismatch, SQL connectivity.

### LoadPagesToProcessTable
- **Signature**: `LoadPagesToProcessTable(AmopLambdaContext context, long serviceProviderId, long inventoryId, int totalPages)`
- **Purpose**: Seeds the page-to-process tracking table with one row per page.
- **Inputs**:
  - `context`, `serviceProviderId`, `inventoryId`, `totalPages`
- **Behavior**:
  - Builds `DataTable` rows for each page with `PageNumber`, `InventoryId`, `ServiceProviderId`.
  - Bulk inserts into `DatabaseTableNames.POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS` via `SqlBulkCopy`.
- **Outputs**: None.
- **Side Effects**: SQL bulk insert.

### InitGetBillingGroupPages
- **Signature**: `InitGetBillingGroupPages(AmopLambdaContext context, long serviceProviderId, long inventoryId, int pageNumber)`
- **Purpose**: Enqueues a work message for a specific page to the get-billing-groups SQS queue.
- **Inputs**:
  - `context`, `serviceProviderId`, `inventoryId`, `pageNumber`
- **Behavior**:
  - Sends an SQS message to `POND_GET_BILLING_GROUPS_QUEUE_URL` with attributes:
    - `SERVICE_PROVIDER_ID`, `INVENTORY_ID`, `PAGE_NUMBER`.
- **Outputs**: None.
- **Side Effects**: SQS publish.

### CheckSyncBillingGroupStepProgress
- **Signature**: `CheckSyncBillingGroupStepProgress(AmopLambdaContext context, SqsValues sqsValues, bool isSuccessful)`
- **Purpose**: Notifies downstream processing of page result/progress.
- **Inputs**:
  - `context`, `sqsValues`, `isSuccessful`
- **Behavior**:
  - Sends an SQS message to `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL` with attributes:
    - `SERVICE_PROVIDER_ID`, `INVENTORY_ID`, `PAGE_NUMBER`, `IS_SUCCESSFUL`.
- **Outputs**: None.
- **Side Effects**: SQS publish.

### GetMessageValues
- **Signature**: `GetMessageValues(SQSEvent.SQSMessage message)`
- **Purpose**: Parses SQS attributes from an incoming message into `SqsValues`.
- **Inputs**:
  - `message`: SQS message
- **Behavior**:
  - Reads attributes using keys from `SQSMessageKeyConstant`:
    - `SERVICE_PROVIDER_ID`, `INVENTORY_ID`, `PAGE_NUMBER`, `IS_SUCCESSFUL`.
  - Converts to typed `SqsValues`.
- **Outputs**: `SqsValues` instance.
- **Errors**: Missing or invalid attributes in processing mode.

### TryGetAllEnvironmentVariables
- **Signature**: `TryGetAllEnvironmentVariables(AmopLambdaContext context)`
- **Purpose**: Reads all Lambda, API, and sync configuration from environment variables.
- **Reads**:
  - `POND_GET_BILLING_GROUPS_QUEUE_URL`
  - `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL`
  - `POND_GET_BILLING_GROUP_ENDPOINT`
  - `PAGE_SIZE` (with default fallback)
- **Outputs**: Populates context/config.
- **Errors**: Missing required variables.

### InitializeRepositories
- **Signature**: `InitializeRepositories(AmopLambdaContext context)`
- **Purpose**: Creates instances of `PondRepository` and `ServiceProviderRepository` using `CentralDbConnectionString`.
- **Inputs**:
  - `context`
- **Outputs**: Repository instances wired to the central DB.
- **Side Effects**: Opens connections as needed.

### PondApiService.TryGetTotalPageCount
- **Signature**: `TryGetTotalPageCount<T>(HttpClient httpClient, string formattedEndpoint, int pageSize)`
- **Purpose**: Returns the total number of pages for a list endpoint.
- **Inputs**:
  - `httpClient`, `formattedEndpoint`, `pageSize`
- **Behavior**:
  - Performs an API call to determine total count and computes `totalPages`.
- **Outputs**: `int totalPages`.
- **Errors**: HTTP failures.

### PondApiService.GetPondListAsync
- **Signature**: `GetPondListAsync<TListResponse>(HttpClient httpClient, string formattedEndpoint, int offset, int pageSize)`
- **Purpose**: Executes a paged list request against Pond endpoints.
- **Outputs**: `TListResponse` with an `Elements` collection.

### PondRepository Methods (selected)
- **GetPondAuthentication(serviceProviderId: long)**: Returns auth/credentials for Pond per service provider.
- **GetAllInventoryIds(serviceProviderId: long)**: Returns all Inventory IDs associated to the service provider.
- **TruncateStagingTables()**: Clears staging tables for a fresh sync.
- **UpdateBillingGroupsPageStatusAndCheckSyncProgress(...)**: Optional downstream progress tracking (mentioned, if applicable).

### ServiceProviderRepository Methods (selected)
- **GetAllServiceProviderIds(integrationType: IntegrationType)**: Enumerates all SP IDs configured for Pond integration.

### RetryPolicyHelper
- **Signature**: `CreateSqlTransientRetryPolicy()` (representative)
- **Purpose**: Supplies a Polly-style retry policy for transient SQL exceptions used by `SyncBillingGroup`.

### AwsFunctionBase Helpers
- **BaseAmopFunctionHandler()**: Creates and initializes `AmopLambdaContext` with logging and config.
- **CleanUp()**: Ensures connections/clients are disposed and any final logging is done.
- **Bulk copy helpers**: Utilities to execute `SqlBulkCopy` operations in a consistent, performant manner.

### SqsService Helpers
- **PublishAsync(queueUrl: string, attributes: IDictionary<string, string>)**: Sends an SQS message to the given queue with attributes used by downstream steps.

## Control Flow Details
- **Initialization path** (no `ServiceProviderId` in SQS message):
  - Truncate staging → enumerate SPs → enumerate inventories → compute `totalPages` → seed page markers → enqueue N SQS messages (one per page).
- **Processing path** (has `ServiceProviderId`):
  - Auth → create `PondApiService` → fetch page (`offset = page * pageSize`) → bulk copy to staging → emit progress to processing queue.

## Error Handling
- **API/HTTP**: `PondApiService` surfaces errors; caller may log and continue per policy.
- **SQL**: `RetryPolicyHelper` used to mitigate transient errors during bulk copy.
- **SQS**: Publish failures are logged; message may be retried by Lambda/SQS redrive policies.

## Notes
- Page-to-process tracking is staged into `POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS`; downstream components can update progress via repository methods as applicable.
- `IS_SUCCESSFUL` attribute is used solely for downstream signaling; it is not required to start processing a page.