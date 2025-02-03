using Catalog.Infrastructure.IntegrationEvent;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Nodes;
using MassTransit;
using Search.Models;

namespace Search.Infrastructure.Cunsumers;

public class CatalogItemAddedEventConsumer(ElasticsearchClient elasticsearchClient)
    : IConsumer<CatalogItemAddedEvent>
{

    private readonly ElasticsearchClient _elasticsearchClient = elasticsearchClient;
    public async Task Consume(ConsumeContext<CatalogItemAddedEvent> context)
    {
        
        var message=context.Message;

        if (message is null) return;


        var itemIndex = new CatalogItemIndex
        {

            CatalogBrand = message.CatalogBrand,
            CatalogCategory = message.CatalogCategory,
            Description = message.Description,
            hintUrl = message.hintUrl,
            Name = message.Name,
            Slug = message.Slug,
        };


        var result =await _elasticsearchClient.Indices.ExistsAsync(CatalogItemIndex.IndexName);

        if(!result.Exists)
        {
            await _elasticsearchClient.Indices.CreateAsync<CatalogItemIndex>(index:CatalogItemIndex.IndexName);
        }

        var response=await _elasticsearchClient.IndexAsync(itemIndex,index:CatalogItemIndex.IndexName);

        if (response.IsValidResponse)
        {
            Console.WriteLine($"Index document with ID {response.Id} succeeded.");
        }


    }
}



