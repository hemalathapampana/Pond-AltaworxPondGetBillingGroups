### AltaworxPondGetBillingGroups Lambda Flow Documentation

#### Overview
The AltaworxPondGetBillingGroups Lambda function synchronizes billing group data from the Pond API into the database. It can initialize a billing group sync session (seeding page work by inventory) and then process billing group pages in batches via SQS messages.

#### HIGH-LEVEL FLOW (Sequential Function Flow)

- **Main Entry Point**
  - `FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)`
    - Receives SQS event and Lambda context
    - Initializes base function handler
    - Iterates through SQS records and routes per-message

- **Initialization Flow (ServiceProviderId not supplied in SQS message)**
  - `InitializeSyncBillingGroupProcess`
    - `TruncateStagingTables` (staging reset)
    - `GetAllServiceProviderIds(IntegrationType.Pond)`
    - For each Service Provider (SP):
      - `GetPondAuthentication`
      - `GetAllInventoryIds` (enumerate inventories for the SP)
      - For each Inventory:
        - TryGetTotalPageCount via Pond API (`PondGetBillingGroupEndpoint`, `PageSize`)
        - `LoadPagesToProcessTable` (seed page markers with `ServiceProviderId` and `InventoryId`)
        - `InitGetBillingGroupPages` (enqueue one SQS message per page)
      - Shape

- **Processing Flow (ServiceProviderId supplied in SQS message)**
  - `ProcessSyncPageByServiceProviderId`
    - `GetPondAuthentication`
    - Instantiate `PondApiService`
    - `SyncBillingGroup`
      - `GetSinglePageListFromPondAPIAsync` (paged fetch)
      - `LoadBillingGroupToStagingTable` (bulk copy to staging)
      - `CheckSyncBillingGroupStepProgress` (emit progress/completion message)
    - Shape

#### LOW-LEVEL FLOW (Detailed Method Explanations)

- **FunctionHandler (Main Entry Point)**
  - Input: `SQSEvent sqsEvent`, `ILambdaContext context`
  - Purpose: Processes SQS messages to orchestrate billing group synchronization
  - What happens:
    - Initializes `AmopLambdaContext` via `BaseAmopFunctionHandler()`
    - Reads environment variables via `TryGetAllEnvironmentVariables()`:
      - `POND_GET_BILLING_GROUPS_QUEUE_URL` (`PondHelper.CommonString.POND_GET_BILLING_GROUPS_QUEUE_URL_VARIABLE_KEY`)
      - `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL` (`PondHelper.CommonString.POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL_VARIABLE_KEY`)
      - `POND_GET_BILLING_GROUP_ENDPOINT` (`PondHelper.CommonString.POND_GET_BILLING_GROUP_ENDPOINT_VARIABLE_KEY`)
      - `PAGE_SIZE` (`PondHelper.CommonString.PAGE_SIZE`, default `PondHelper.CommonConfig.DEFAULT_PAGE_SIZE`)
    - Ensures SQS trigger validity and iterates each record
    - For each record:
      - Logs diagnostics
      - Parses attributes with `GetMessageValues()`:
        - `ServiceProviderId` (required for processing mode)
        - `InventoryId` (required for processing mode)
        - `PageNumber` (required for processing mode)
        - `IsSuccessful` (used in downstream stage processing queue)
      - If `ServiceProviderId <= 0` or missing: routes to `InitializeSyncBillingGroupProcess()`
      - Else: routes to `ProcessSyncPageByServiceProviderId()`
    - Handles exceptions and calls `CleanUp()`

- **InitializeSyncBillingGroupProcess (Initialization Mode)**
  - Input: `AmopLambdaContext context`, `ServiceProviderRepository serviceProviderRepository`
  - Purpose: Seeds the sync process (by inventory) and fans out processing across pages
  - What happens:
    - Reset staging via `pondRepository.TruncateStagingTables`
    - Retrieve all Pond service provider IDs via `GetAllServiceProviderIds`
    - For each `serviceProviderId`:
      - Retrieve auth via `pondRepository.GetPondAuthentication`
      - Retrieve all inventory IDs via `pondRepository.GetAllInventoryIds`
      - For each `inventoryId`:
        - Build formatted endpoint via `string.Format(PondGetBillingGroupEndpoint, inventoryId)`
        - Call API once to get total page count via `pondApiService.TryGetTotalPageCount<PondBillingGroupItem>` using the formatted endpoint
        - `LoadPagesToProcessTable(context, serviceProviderId, inventoryId, totalPages)` seeds DB page markers (`POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS`)
        - For page in [0, totalPages):
          - `InitGetBillingGroupPages(context, serviceProviderId, inventoryId, page)` enqueues SQS message to `POND_GET_BILLING_GROUPS_QUEUE_URL`
    - Shape

- **ProcessSyncPageByServiceProviderId (Processing Mode)**
  - Input: `AmopLambdaContext context`, `SqsValues sqsValues`
  - Purpose: Pulls one page of billing groups for an inventory from Pond and loads to staging
  - What happens:
    - Retrieve auth via `pondRepository.GetPondAuthentication`
    - Create `PondApiService`
    - `SyncBillingGroup(context, sqsValues, sqlTransientRetryPolicy, pondApiService)`
      - Calls `GetSinglePageListFromPondAPIAsync<PondBillingGroupItem, PondBillingGroupListResponse>`:
        - Calculates `offset = pageNumber * PageSize`
        - Fetches from Pond via `GetPondListAsync<PondBillingGroupListResponse>(HttpClientSingleton.Instance, formattedEndpoint, offset, PageSize)`
        - Extracts list via `response => response.Elements`
        - `LoadBillingGroupToStagingTable` builds a `DataTable` and executes `SqlBulkCopy` to `PondBillingGroupStaging`
        - On each page, calls `CheckSyncBillingGroupStepProgress` with `IsSuccessful`, which emits an SQS message to `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL` for downstream processing
    - Shape

#### Utility Functions
- **GetMessageValues**
  - Parses SQS attributes from the incoming message into `SqsValues`
  - Attributes used (via `SQSMessageKeyConstant`):
    - `SERVICE_PROVIDER_ID`
    - `INVENTORY_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL` (used for progress/completion signaling downstream)

- **TryGetAllEnvironmentVariables**
  - Reads Lambda, API, and sync configuration from environment variables:
    - `POND_GET_BILLING_GROUPS_QUEUE_URL`
    - `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL`
    - `POND_GET_BILLING_GROUP_ENDPOINT`
    - `PAGE_SIZE`

- **InitializeRepositories**
  - Instantiates `PondRepository` and `ServiceProviderRepository` using `CentralDbConnectionString`

- **LoadBillingGroupToStagingTable**
  - Shapes `DataTable` schema with columns: `Id`, `Parent_Group_Id`, `Ocs_Group_Id`, `Inventory_Id`, `Name`, `Number_Of_Subgroups`, `Number_Of_Sims`, `CreatedDate`, `ServiceProviderId`
  - Executes `SqlBulkCopy` into `DatabaseTableNames.PondBillingGroupStaging`

- **LoadPagesToProcessTable**
  - Builds `DataTable` with `PageNumber`, `InventoryId`, and `ServiceProviderId` for all pages
  - Executes `SqlBulkCopy` into `DatabaseTableNames.POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS`

- **InitGetBillingGroupPages**
  - Sends an SQS message per page to `POND_GET_BILLING_GROUPS_QUEUE_URL` with attributes:
    - `SERVICE_PROVIDER_ID`
    - `INVENTORY_ID`
    - `PAGE_NUMBER`

- **CheckSyncBillingGroupStepProgress**
  - Sends an SQS message to `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL` with attributes:
    - `SERVICE_PROVIDER_ID`
    - `INVENTORY_ID`
    - `PAGE_NUMBER`
    - `IS_SUCCESSFUL`

- **SyncBillingGroup**
  - Orchestrates single-page fetch, staging load, and progress signaling using retry policy

#### Key Dependencies and Integrations
- **AwsFunctionBase**: logging, config, DB connections, bulk copy, cleanup
- **PondRepository**: DB CRUD for Pond sync, staging, and progress tracking
- **PondApiService**: list API calls, query param construction, request building
- **ServiceProviderRepository**: service provider enumeration and metadata
- **EnvironmentRepository**: environment variable access
- **SqsService**: SQS message publishing
- **RetryPolicyHelper**: SQL transient retry policy
- **HttpClientSingleton and HttpRequestFactory**: HTTP client and request construction
- Shape

#### Data Flow Summary
- **Initialization**: seed page markers per service provider and inventory; enqueue SQS messages per page
- **Fetch**: pull one page of billing groups from Pond using `offset = pageNumber * pageSize`
- **Stage**: bulk insert to `PondBillingGroupStaging`
- **Advance**: emit progress messages to `POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL` for downstream processing
- **Note**: Page-to-process tracking is staged into `POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS`; downstream components can update progress via repository methods (e.g., `UpdateBillingGroupsPageStatusAndCheckSyncProgress`) as applicable
- Shape

#### Method Implementations (Detailed Explanations)

- **PondRepository.GetPondAuthentication**
  - Signature: `public virtual PondAuthentication GetPondAuthentication(Action<string, string> logFunction, IBase64Service base64Service, int serviceProviderId = 0)`
  - Purpose: Retrieves Pond API authentication/tenant context (base URLs, distributor, credentials) for a given Service Provider.
  - How it works:
    - Builds parameters with `SERVICE_PROVIDER_ID`.
    - Executes stored procedure `GET_POND_AUTHENTICATION` under SQL retry policy.
    - Maps each row to `PondAuthentication` using the provided `IBase64Service`.
    - Returns the first authentication record (or `null` if none).
  - Notes:
    - Passing `serviceProviderId = 0` lets the stored procedure decide default behavior (if any).
    - The result determines Production vs Sandbox base URLs used in API calls.

- **Function.SyncBillingGroup**
  - Signature: `protected async Task SyncBillingGroup(AmopLambdaContext context, SqsValues sqsValues, ISyncPolicy syncPolicy, PondApiService pondApiService)`
  - Purpose: Orchestrates fetching one page of billing groups for a specific inventory and loading them to staging, then signaling progress.
  - How it works:
    - Builds the endpoint: `formattedEndpoint = string.Format(PondGetBillingGroupEndpoint, sqsValues.InventoryId)`.
    - Invokes `pondApiService.GetSinglePageListFromPondAPIAsync<PondBillingGroupItem, PondBillingGroupListResponse>` with delegates:
      - `getFromAPIFunc(offset, pageSize)`: `pondApiService.GetPondListAsync<PondBillingGroupListResponse>(HttpClientSingleton.Instance, formattedEndpoint, offset, pageSize)`
      - `getListFromResponseFunc(response)`: `response.Elements`
      - `loadDataToStagingFunc(list)`: `LoadBillingGroupToStagingTable(context, list, sqsValues.ServiceProviderId)`
      - `checkSyncStepProgressFunc(pageNumber, isSuccess)`: `CheckSyncBillingGroupStepProgress(context, sqsValues.ServiceProviderId, sqsValues.InventoryId, pageNumber, isSuccess)`
  - Notes:
    - Offset is derived internally as `pageNumber * PageSize` by the `GetSinglePageListFromPondAPIAsync` helper.
    - The `syncPolicy` wraps only the staging load; HTTP requests are not retried by this policy.

- **PondApiService.GetSinglePageListFromPondAPIAsync<TListItem, TAPIResponse>**
  - Signature: `public async Task GetSinglePageListFromPondAPIAsync<TListItem, TAPIResponse>(Action<string, string> logFunction, ISyncPolicy sqlRetryPolicy, int pageNumber, int pageSize, Func<int, int, Task<TAPIResponse>> getFromAPIFunc, Func<TAPIResponse, List<TListItem>> getListFromResponseFunc, Action<List<TListItem>> loadDataToStagingFunc, Func<int, bool, Task> checkSyncStepProgressFunc)`
  - Purpose: Generic helper to fetch a single page from Pond, transform results, persist to staging, and publish progress.
  - How it works:
    - Logs a sub-step marker via `logFunction`.
    - Computes `offset = pageNumber * pageSize` and awaits `getFromAPIFunc(offset, pageSize)`.
    - Initializes `isSuccess = false`; sets to `true` when the API response is non-null and list extraction returns a non-null list (empty list is treated as success).
    - On null API response, logs `ERROR_NULL_API_REPONSE`.
    - Executes `loadDataToStagingFunc(elements)` within `sqlRetryPolicy`.
    - Awaits `checkSyncStepProgressFunc(pageNumber, isSuccess)` to signal downstream progress.
  - Notes:
    - This method deliberately treats “no elements” as success to advance progress tracking even when a page is empty.
    - Any exceptions in staging load are handled by the SQL retry policy; callers should handle outer exceptions as needed.

- **PondApiService.GetPondListAsync<T>**
  - Signature: `public async Task<T> GetPondListAsync<T>(HttpClient httpClient, string endpoint, int offset = 0, int pageSize = PondHelper.CommonConfig.DEFAULT_PAGE_SIZE, IKeysysLogger logger = null)`
  - Purpose: Performs the actual HTTP GET to Pond with pagination query parameters and deserializes the response.
  - How it works:
    - Logs the request tuple `(endpoint, offset, pageSize)` when a logger is provided.
    - Chooses base URI based on environment: Production vs Sandbox from `PondAuthentication`.
    - Builds query params via `BuildQueryParamGetInventoryList(offset, pageSize)` and appends to `{baseUri}/{DistributorId}/{endpoint}`.
    - Creates a GET request with `BuildRequestMessage` and sends it using the provided `httpClient`.
    - Reads `responseBody` and logs error content when the status code is non-success.
    - Deserializes JSON into `T` via `JsonConvert.DeserializeObject<T>(responseBody)` and returns it.
  - Notes:
    - Non-success HTTP responses are logged but not thrown; callers must interpret `null` or failed deserialization accordingly.
    - No built-in HTTP retry is applied here; consider wrapping at the call site if needed.

- **Function.CheckSyncBillingGroupStepProgress**
  - Signature: `private async Task CheckSyncBillingGroupStepProgress(AmopLambdaContext context, int serviceProviderId, int inventoryId, int currentPage, bool isSuccess)`
  - Purpose: Publishes a progress message to the downstream processing queue so other steps can update status and continue the pipeline.
  - How it works:
    - Builds SQS message attributes:
      - `SERVICE_PROVIDER_ID`: `serviceProviderId`
      - `INVENTORY_ID`: `inventoryId`
      - `PAGE_NUMBER`: `currentPage`
      - `IS_SUCCESSFUL`: `isSuccess`
    - Sends the message to `ProcessStagedBillingGroupsQueueURL` via `SqsService.SendSQSMessage`.
  - Notes:
    - Downstream processors can call `PondRepository.UpdateBillingGroupsPageStatusAndCheckSyncProgress` to persist progress and determine overall completion.
