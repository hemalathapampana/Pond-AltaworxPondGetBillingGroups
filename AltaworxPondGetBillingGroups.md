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