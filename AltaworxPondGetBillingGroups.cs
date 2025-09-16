using System.Data;
using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Altaworx.AWS.Core.Services.SQS;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Pond;
using Amop.Core.Models;
using Amop.Core.Models.Pond;
using Amop.Core.Repositories;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Pond;
using Amop.Core.Services.Http;
using Amop.Core.Services.Pond;
using Microsoft.Data.SqlClient;
using Polly;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxPondGetBillingGroups;

public class Function : AwsFunctionBase
{
    private int PageSize;
    // SQS Queue URL that is connected to the AltaworxPondGetDevices lambda
    private string GetBillingGroupsQueueURL = string.Empty;
    private string ProcessStagedBillingGroupsQueueURL = string.Empty;
    private string PondGetBillingGroupEndpoint = string.Empty;
    protected PondRepository pondRepository;
    protected ServiceProviderRepository serviceProviderRepository;
    protected SqsService sqsService = new SqsService();
    private readonly HttpRequestFactory _httpRequestFactory = new HttpRequestFactory();
    private readonly EnvironmentRepository _environmentRepo = new EnvironmentRepository();
    public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {
        AmopLambdaContext? lambdaContext = null;
        try
        {
            lambdaContext = BaseAmopFunctionHandler(context);
            ArgumentNullException.ThrowIfNull(lambdaContext);

            InitializeRepositories(lambdaContext);

            TryGetAllEnvironmentVariables(lambdaContext);

            await ProcessEventAsync(lambdaContext, sqsEvent);
        }
        catch (Exception ex)
        {
            if (lambdaContext == null)
            {
                context.Logger.Log(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
            else
            {
                LogInfo(lambdaContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
        }

        base.CleanUp(lambdaContext);
    }

    protected void InitializeRepositories(AmopLambdaContext lambdaContext)
    {
        pondRepository = new PondRepository(lambdaContext.CentralDbConnectionString);
        serviceProviderRepository = new ServiceProviderRepository(lambdaContext.CentralDbConnectionString);
    }

    protected void TryGetAllEnvironmentVariables(AmopLambdaContext lambdaContext)
    {
        // Lambda related configurations
        GetBillingGroupsQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_BILLING_GROUPS_QUEUE_URL_VARIABLE_KEY);
        ProcessStagedBillingGroupsQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_PROCESS_STAGED_BILLING_GROUPS_QUEUE_URL_VARIABLE_KEY);
        // API related configurations
        PondGetBillingGroupEndpoint = GetStringValueFromEnvironmentVariable(lambdaContext.Context, _environmentRepo, PondHelper.CommonString.POND_GET_BILLING_GROUP_ENDPOINT_VARIABLE_KEY);
        // Sync logic related configurations
        PageSize = GetIntValueFromEnvironmentVariable(lambdaContext, _environmentRepo,
            PondHelper.CommonString.PAGE_SIZE,
            PondHelper.CommonConfig.DEFAULT_PAGE_SIZE);
    }

    private SqsValues GetMessageValues(AmopLambdaContext context, SQSEvent.SQSMessage message)
    {
        return new SqsValues(context, message);
    }

    private async Task ProcessEventAsync(AmopLambdaContext context, SQSEvent sqsEvent)
    {
        LogInfo(context, CommonConstants.SUB);
        if (sqsEvent?.Records != null)
        {
            var processedRecordCount = sqsEvent.Records.Count;
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.BEGINNING_PROCESS, processedRecordCount));
            foreach (var record in sqsEvent.Records)
            {
                LogInfo(context, CommonConstants.INFO, $"MessageId: {record.MessageId}");
                var sqsValues = GetMessageValues(context, record);
                if (sqsValues.ServiceProviderId <= 0)
                {
                    // No service provider id provided -> Initialize sync process using sqs message
                    await InitializeSyncBillingGroupProcess(context, serviceProviderRepository);
                }
                else
                {
                    // Run for the current service provider id (specified in the SQS Message)
                    await ProcessSyncPageByServiceProviderId(context, sqsValues);
                }
            }
        }
        else
        {
            await InitializeSyncBillingGroupProcess(context, serviceProviderRepository);
        }
    }

    protected async Task InitializeSyncBillingGroupProcess(AmopLambdaContext context, ServiceProviderRepository serviceProviderRepository)
    {
        // Clean staging table
        var errorMessages = new List<string>();
        var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
        sqlTransientRetryPolicy.Execute(() => pondRepository.TruncateStagingTables(ParameterizedLog(context)));
        var serviceProviderIds = serviceProviderRepository.GetAllServiceProviderIds(ParameterizedLog(context), IntegrationType.Pond);
        if (serviceProviderIds?.Count > 0)
        {
            foreach (var serviceProviderId in serviceProviderIds)
            {
                var pondAuth = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, serviceProviderId);
                if (pondAuth == null)
                {
                    LogInfo(context, CommonConstants.WARNING, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, serviceProviderId));
                    continue;
                }

                // Get all inventory ids
                var inventoryIds = pondRepository.GetAllInventoryIds(ParameterizedLog(context), serviceProviderId);
                var pondApiService = new PondApiService(pondAuth, _httpRequestFactory, context.IsProduction);

                foreach (var inventoryId in inventoryIds)
                {
                    var formattedEndpoint = string.Format(PondGetBillingGroupEndpoint, inventoryId);
                    // Call API once each inventory id to get total pages
                    var totalPages = await pondApiService.TryGetTotalPageCount<PondBillingGroupItem>(ParameterizedLog(context), formattedEndpoint, PageSize);
                    if (totalPages > 0)
                    {
                        LoadPagesToProcessTable(context, serviceProviderId, inventoryId, totalPages);
                        // Page number start from 0 since the API need to be query by offset, which start by 0
                        // (first item of page = offset + pageNumber * pageSize)
                        for (var i = 0; i < totalPages; i++)
                        {
                            await InitGetBillingGroupPages(context, serviceProviderId, inventoryId, i);
                        }
                    }
                }
            }
        }
        else
        {
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NO_SERVICE_PROVIDER_FOUND, CommonConstants.POND_CARRIER_NAME));
        }
    }

    protected async Task ProcessSyncPageByServiceProviderId(AmopLambdaContext context, SqsValues sqsValues)
    {
        try
        {
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
            var pondAuth = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, sqsValues.ServiceProviderId);
            if (pondAuth == null)
            {
                LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, sqsValues.ServiceProviderId));
                return;
            }

            var pondApiService = new PondApiService(pondAuth, _httpRequestFactory, context.IsProduction);
            await SyncBillingGroup(context, sqsValues, sqlTransientRetryPolicy, pondApiService);
        }
        catch (Exception ex)
        {
            LogInfo(context, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
        }
    }

    protected async Task SyncBillingGroup(AmopLambdaContext context, SqsValues sqsValues, ISyncPolicy syncPolicy, PondApiService pondApiService)
    {
        var formattedEndpoint = string.Format(PondGetBillingGroupEndpoint, sqsValues.InventoryId);
        await pondApiService.GetSinglePageListFromPondAPIAsync<PondBillingGroupItem, PondBillingGroupListResponse>(ParameterizedLog(context), syncPolicy, sqsValues.PageNumber, PageSize,
            (offset, pageSize) => pondApiService.GetPondListAsync<PondBillingGroupListResponse>(HttpClientSingleton.Instance, formattedEndpoint, offset, pageSize),
            (response) => response.Elements,
            (response) => LoadBillingGroupToStagingTable(context, response, sqsValues.ServiceProviderId),
            async (pageNumber, isSuccess) => await CheckSyncBillingGroupStepProgress(context, sqsValues.ServiceProviderId, sqsValues.InventoryId, pageNumber, isSuccess));
    }

    protected static void LoadBillingGroupToStagingTable(AmopLambdaContext context, List<PondBillingGroupItem> pondBillingGroups, int serviceProviderId)
    {
        LogInfo(context, CommonConstants.SUB);
        var pondBillingGroupTable = new DataTable();
        pondBillingGroupTable.Columns.Add(CommonColumnNames.Id, typeof(int));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.Parent_Group_Id, typeof(int));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.Ocs_Group_Id, typeof(int));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.Inventory_Id, typeof(int));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.Name, typeof(string));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.Number_Of_Subgroups, typeof(int));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.Number_Of_Sims, typeof(int));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.CreatedDate, typeof(DateTime));
        pondBillingGroupTable.Columns.Add(CommonColumnNames.ServiceProviderId, typeof(int));

        foreach (var pondBillingGroupItem in pondBillingGroups)
        {
            var pondBillingGroupRow = pondBillingGroupTable.NewRow();
            pondBillingGroupRow[CommonColumnNames.Id] = pondBillingGroupItem.Id;
            pondBillingGroupRow[CommonColumnNames.Parent_Group_Id] = pondBillingGroupItem.ParentGroupId ?? (object)DBNull.Value;
            pondBillingGroupRow[CommonColumnNames.Ocs_Group_Id] = pondBillingGroupItem.OcsGroupId ?? (object)DBNull.Value;
            pondBillingGroupRow[CommonColumnNames.Inventory_Id] = pondBillingGroupItem.InventoryId;
            pondBillingGroupRow[CommonColumnNames.Name] = pondBillingGroupItem.Name ?? (object)DBNull.Value;
            pondBillingGroupRow[CommonColumnNames.Number_Of_Subgroups] = pondBillingGroupItem.NumberOfSubgroups;
            pondBillingGroupRow[CommonColumnNames.Number_Of_Sims] = pondBillingGroupItem.NumberOfSims;
            pondBillingGroupRow[CommonColumnNames.CreatedDate] = DateTime.UtcNow;
            pondBillingGroupRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            pondBillingGroupTable.Rows.Add(pondBillingGroupRow);
        }

        List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(pondBillingGroupTable);
        SqlBulkCopy(context, context.CentralDbConnectionString, pondBillingGroupTable, DatabaseTableNames.PondBillingGroupStaging, columnMappings);
    }

    protected static void LoadPagesToProcessTable(AmopLambdaContext context, int serviceProviderId, int inventoryId, int totalPages)
    {
        LogInfo(context, CommonConstants.SUB);
        var pondBillingGroupPageTable = new DataTable();
        pondBillingGroupPageTable.Columns.Add(CommonColumnNames.PageNumber, typeof(int));
        pondBillingGroupPageTable.Columns.Add(CommonColumnNames.InventoryId, typeof(int));
        pondBillingGroupPageTable.Columns.Add(CommonColumnNames.ServiceProviderId, typeof(int));

        for (var i = 0; i < totalPages; i++)
        {
            var pondBillingGroupPageRow = pondBillingGroupPageTable.NewRow();
            pondBillingGroupPageRow[CommonColumnNames.PageNumber] = i;
            pondBillingGroupPageRow[CommonColumnNames.InventoryId] = inventoryId;
            pondBillingGroupPageRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            pondBillingGroupPageTable.Rows.Add(pondBillingGroupPageRow);
        }

        List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(pondBillingGroupPageTable);
        SqlBulkCopy(context, context.CentralDbConnectionString, pondBillingGroupPageTable, DatabaseTableNames.POND_GET_BILLING_GROUPS_PAGE_TO_PROCESS, columnMappings);
    }

    private async Task InitGetBillingGroupPages(AmopLambdaContext context, int serviceProviderId, int inventoryId, int currentPage)
    {
        // Insert all the pages to table
        var attributes = new Dictionary<string, string>()
        {
            {SQSMessageKeyConstant.SERVICE_PROVIDER_ID, serviceProviderId.ToString()},
            {SQSMessageKeyConstant.INVENTORY_ID, inventoryId.ToString()},
            {SQSMessageKeyConstant.PAGE_NUMBER, currentPage.ToString()},
        };
        await sqsService.SendSQSMessage(ParameterizedLog(context), AwsCredentials(context), GetBillingGroupsQueueURL, attributes);
    }

    private async Task CheckSyncBillingGroupStepProgress(AmopLambdaContext context, int serviceProviderId, int inventoryId, int currentPage, bool isSuccess)
    {
        var attributes = new Dictionary<string, string>()
        {
            {SQSMessageKeyConstant.SERVICE_PROVIDER_ID, serviceProviderId.ToString()},
            {SQSMessageKeyConstant.INVENTORY_ID, inventoryId.ToString()},
            {SQSMessageKeyConstant.PAGE_NUMBER, currentPage.ToString()},
            {SQSMessageKeyConstant.IS_SUCCESSFUL, isSuccess.ToString()},
        };

        await sqsService.SendSQSMessage(ParameterizedLog(context), AwsCredentials(context), ProcessStagedBillingGroupsQueueURL, attributes);
    }
}